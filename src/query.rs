use crate::sync::{Arc, RwLock};
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hash_types::{BlockHash, TxMerkleNode, Txid};
use bitcoin::hashes::hex::ToHex;
use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use bitcoin::hashes::Hash;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::app::App;
use crate::cache::TransactionCache;
use crate::errors::*;
use crate::index::{compute_script_hash, TxInRow, TxOutRow, TxRow};
use crate::mempool::Tracker;
use crate::metrics::{HistogramOpts, HistogramVec, Metrics};
use crate::store::{ReadStore, Row};
use crate::util::{FullHash, HashPrefix, HeaderEntry};

pub struct FundingOutput {
    pub txn_id: Txid,
    pub height: u32,
    pub output_index: usize,
    pub value: u64,
}

type OutPoint = (Txid, usize); // (txid, output_index)

struct SpendingInput {
    txn_id: Txid,
    height: u32,
    funding_output: OutPoint,
    value: u64,
}

pub struct Status {
    confirmed: (Vec<FundingOutput>, Vec<SpendingInput>),
    mempool: (Vec<FundingOutput>, Vec<SpendingInput>),
    txn_fees: HashMap<Txid, u64>,
}

fn calc_balance((funding, spending): &(Vec<FundingOutput>, Vec<SpendingInput>)) -> i64 {
    let funded: u64 = funding.iter().map(|output| output.value).sum();
    let spent: u64 = spending.iter().map(|input| input.value).sum();
    funded as i64 - spent as i64
}

pub struct HistoryItem {
    height: i32,
    tx_hash: Txid,
    fee: Option<u64>, // need to be set only for unconfirmed transactions (i.e. height <= 0)
}

impl HistoryItem {
    pub fn to_json(&self) -> Value {
        let mut result = json!({ "height": self.height, "tx_hash": self.tx_hash.to_hex()});
        self.fee.map(|f| {
            result
                .as_object_mut()
                .unwrap()
                .insert("fee".to_string(), json!(f))
        });
        result
    }
}

impl Status {
    fn funding(&self) -> impl Iterator<Item = &FundingOutput> {
        self.confirmed.0.iter().chain(self.mempool.0.iter())
    }

    fn spending(&self) -> impl Iterator<Item = &SpendingInput> {
        self.confirmed.1.iter().chain(self.mempool.1.iter())
    }

    pub fn confirmed_balance(&self) -> i64 {
        calc_balance(&self.confirmed)
    }

    pub fn mempool_balance(&self) -> i64 {
        calc_balance(&self.mempool)
    }

    pub fn history(&self) -> Vec<HistoryItem> {
        let mut txns_map = HashMap::<Txid, i32>::new();
        for f in self.funding() {
            txns_map.insert(f.txn_id, f.height as i32);
        }
        for s in self.spending() {
            txns_map.insert(s.txn_id, s.height as i32);
        }
        let mut items: Vec<HistoryItem> = txns_map
            .into_iter()
            .map(|item| HistoryItem {
                height: item.1,
                tx_hash: item.0,
                fee: self.txn_fees.get(&item.0).cloned(),
            })
            .collect();
        items.sort_unstable_by_key(|item| item.height);
        items
    }

    pub fn unspent(&self) -> Vec<&FundingOutput> {
        let mut outputs_map = HashMap::<OutPoint, &FundingOutput>::new();
        for f in self.funding() {
            outputs_map.insert((f.txn_id, f.output_index), f);
        }
        for s in self.spending() {
            if outputs_map.remove(&s.funding_output).is_none() {
                warn!("failed to remove {:?}", s.funding_output);
            }
        }
        let mut outputs = outputs_map
            .into_iter()
            .map(|item| item.1) // a reference to unspent output
            .collect::<Vec<&FundingOutput>>();
        outputs.sort_unstable_by_key(|out| out.height);
        outputs
    }

    pub fn hash(&self) -> Option<FullHash> {
        let txns = self.history();
        if txns.is_empty() {
            None
        } else {
            let mut sha2 = Sha256::new();
            for item in txns {
                let part = format!("{}:{}:", item.tx_hash.to_hex(), item.height);
                sha2.update(part.as_bytes());
            }
            Some(sha2.finalize().into())
        }
    }
}

struct TxnHeight {
    txn: Transaction,
    height: u32,
}

fn merklize<T: Hash>(left: T, right: T) -> T {
    let data = [&left[..], &right[..]].concat();
    <T as Hash>::hash(&data)
}

fn create_merkle_branch_and_root<T: Hash>(mut hashes: Vec<T>, mut index: usize) -> (Vec<T>, T) {
    let mut merkle = vec![];
    while hashes.len() > 1 {
        if hashes.len() % 2 != 0 {
            let last = *hashes.last().unwrap();
            hashes.push(last);
        }
        index = if index % 2 == 0 { index + 1 } else { index - 1 };
        merkle.push(hashes[index]);
        index /= 2;
        hashes = hashes
            .chunks(2)
            .map(|pair| merklize(pair[0], pair[1]))
            .collect()
    }
    (merkle, hashes[0])
}

// TODO: the functions below can be part of ReadStore.
fn txrow_by_txid(store: &dyn ReadStore, txid: &Txid) -> Option<TxRow> {
    let key = TxRow::filter_full(&txid);
    let value = store.get(&key)?;
    Some(TxRow::from_row(&Row { key, value }))
}

fn txrows_by_prefix(store: &dyn ReadStore, txid_prefix: HashPrefix) -> Vec<TxRow> {
    store
        .scan(&TxRow::filter_prefix(txid_prefix))
        .iter()
        .map(|row| TxRow::from_row(row))
        .collect()
}

fn txids_by_script_hash(store: &dyn ReadStore, script_hash: &[u8]) -> Vec<HashPrefix> {
    store
        .scan(&TxOutRow::filter(script_hash))
        .iter()
        .map(|row| TxOutRow::from_row(row).txid_prefix)
        .collect()
}

fn txids_by_funding_output(
    store: &dyn ReadStore,
    txn_id: &Txid,
    output_index: usize,
) -> Vec<HashPrefix> {
    store
        .scan(&TxInRow::filter(&txn_id, output_index))
        .iter()
        .map(|row| TxInRow::from_row(row).txid_prefix)
        .collect()
}

pub struct Query {
    app: Arc<App>,
    tracker: RwLock<Tracker>,
    tx_cache: TransactionCache,
    txid_limit: usize,
    duration: HistogramVec,
}

impl Query {
    pub fn new(
        app: Arc<App>,
        metrics: &Metrics,
        tx_cache: TransactionCache,
        txid_limit: usize,
    ) -> Arc<Query> {
        Arc::new(Query {
            app,
            tracker: RwLock::new(Tracker::new(metrics)),
            tx_cache,
            txid_limit,
            duration: metrics.histogram_vec(
                HistogramOpts::new("electrs_query_duration", "Request duration (in seconds)"),
                &["type"],
            ),
        })
    }

    fn load_txns_by_prefix(
        &self,
        store: &dyn ReadStore,
        prefixes: Vec<HashPrefix>,
    ) -> Result<Vec<TxnHeight>> {
        let mut txns = vec![];
        for txid_prefix in prefixes {
            for tx_row in txrows_by_prefix(store, txid_prefix) {
                let txid: Txid = deserialize(&tx_row.key.txid).unwrap();
                let txn = self.load_txn(&txid, Some(tx_row.height))?;
                txns.push(TxnHeight {
                    txn,
                    height: tx_row.height,
                })
            }
        }
        Ok(txns)
    }

    fn find_spending_input(
        &self,
        store: &dyn ReadStore,
        funding: &FundingOutput,
    ) -> Result<Option<SpendingInput>> {
        let spending_txns: Vec<TxnHeight> = self.load_txns_by_prefix(
            store,
            txids_by_funding_output(store, &funding.txn_id, funding.output_index),
        )?;
        let mut spending_inputs = vec![];
        for t in &spending_txns {
            for input in t.txn.input.iter() {
                if input.previous_output.txid == funding.txn_id
                    && input.previous_output.vout == funding.output_index as u32
                {
                    spending_inputs.push(SpendingInput {
                        txn_id: t.txn.txid(),
                        height: t.height,
                        funding_output: (funding.txn_id, funding.output_index),
                        value: funding.value,
                    })
                }
            }
        }
        assert!(spending_inputs.len() <= 1);
        Ok(if spending_inputs.len() == 1 {
            Some(spending_inputs.remove(0))
        } else {
            None
        })
    }

    fn find_funding_outputs(&self, t: &TxnHeight, script_hash: &[u8]) -> Vec<FundingOutput> {
        let mut result = vec![];
        let txn_id = t.txn.txid();
        for (index, output) in t.txn.output.iter().enumerate() {
            if compute_script_hash(&output.script_pubkey[..]) == script_hash {
                result.push(FundingOutput {
                    txn_id,
                    height: t.height,
                    output_index: index,
                    value: output.value,
                })
            }
        }
        result
    }

    fn confirmed_status(
        &self,
        script_hash: &[u8],
    ) -> Result<(Vec<FundingOutput>, Vec<SpendingInput>)> {
        let mut funding = vec![];
        let mut spending = vec![];
        let read_store = self.app.read_store();
        let txid_prefixes = txids_by_script_hash(read_store, script_hash);
        // if the limit is enabled
        if self.txid_limit > 0 && txid_prefixes.len() > self.txid_limit {
            bail!(
                "{}+ transactions found, query may take a long time",
                txid_prefixes.len()
            );
        }
        for t in self.load_txns_by_prefix(read_store, txid_prefixes)? {
            funding.extend(self.find_funding_outputs(&t, script_hash));
        }
        for funding_output in &funding {
            if let Some(spent) = self.find_spending_input(read_store, &funding_output)? {
                spending.push(spent);
            }
        }
        Ok((funding, spending))
    }

    fn mempool_status(
        &self,
        script_hash: &[u8],
        confirmed_funding: &[FundingOutput],
        tracker: &Tracker,
    ) -> Result<(Vec<FundingOutput>, Vec<SpendingInput>)> {
        let mut funding = vec![];
        let mut spending = vec![];
        let txid_prefixes = txids_by_script_hash(tracker.index(), script_hash);
        for t in self.load_txns_by_prefix(tracker.index(), txid_prefixes)? {
            funding.extend(self.find_funding_outputs(&t, script_hash));
        }
        // // TODO: dedup outputs (somehow) both confirmed and in mempool (e.g. reorg?)
        for funding_output in funding.iter().chain(confirmed_funding.iter()) {
            if let Some(spent) = self.find_spending_input(tracker.index(), &funding_output)? {
                spending.push(spent);
            }
        }
        Ok((funding, spending))
    }

    pub fn status(&self, script_hash: &[u8]) -> Result<Status> {
        let timer = self
            .duration
            .with_label_values(&["confirmed_status"])
            .start_timer();
        let confirmed = self
            .confirmed_status(script_hash)
            .chain_err(|| "failed to get confirmed status")?;
        timer.observe_duration();

        let tracker = self.tracker.read().unwrap();
        let timer = self
            .duration
            .with_label_values(&["mempool_status"])
            .start_timer();
        let mempool = self
            .mempool_status(script_hash, &confirmed.0, &tracker)
            .chain_err(|| "failed to get mempool status")?;
        timer.observe_duration();

        let mut txn_fees = HashMap::new();
        let funding_txn_ids = mempool.0.iter().map(|funding| funding.txn_id);
        let spending_txn_ids = mempool.1.iter().map(|spending| spending.txn_id);
        for mempool_txid in funding_txn_ids.chain(spending_txn_ids) {
            tracker
                .get_fee(&mempool_txid)
                .map(|fee| txn_fees.insert(mempool_txid, fee));
        }

        Ok(Status {
            confirmed,
            mempool,
            txn_fees,
        })
    }

    fn lookup_confirmed_blockhash(
        &self,
        tx_hash: &Txid,
        block_height: Option<u32>,
    ) -> Result<Option<BlockHash>> {
        let blockhash = if self.tracker.read().unwrap().has_txn(&tx_hash) {
            None // found in mempool (as unconfirmed transaction)
        } else {
            // Lookup in confirmed transactions' index
            let height = match block_height {
                Some(height) => height,
                None => {
                    txrow_by_txid(self.app.read_store(), &tx_hash)
                        .chain_err(|| format!("not indexed tx {}", tx_hash))?
                        .height
                }
            };
            let header = self
                .app
                .index()
                .get_header(height as usize)
                .chain_err(|| format!("missing header at height {}", height))?;
            Some(*header.hash())
        };
        Ok(blockhash)
    }

    // Internal API for transaction retrieval
    fn load_txn(&self, txid: &Txid, block_height: Option<u32>) -> Result<Transaction> {
        let _timer = self.duration.with_label_values(&["load_txn"]).start_timer();
        self.tx_cache.get_or_else(&txid, || {
            let blockhash = self.lookup_confirmed_blockhash(txid, block_height)?;
            let value: Value = self
                .app
                .daemon()
                .gettransaction_raw(txid, blockhash, /*verbose*/ false)?;
            let value_hex: &str = value.as_str().chain_err(|| "non-string tx")?;
            hex::decode(&value_hex).chain_err(|| "non-hex tx")
        })
    }

    // Public API for transaction retrieval (for Electrum RPC)
    pub fn get_transaction(&self, tx_hash: &Txid, verbose: bool) -> Result<Value> {
        let _timer = self
            .duration
            .with_label_values(&["get_transaction"])
            .start_timer();
        let blockhash = self.lookup_confirmed_blockhash(tx_hash, /*block_height*/ None)?;
        self.app
            .daemon()
            .gettransaction_raw(tx_hash, blockhash, verbose)
    }

    pub fn get_confirmed_blockhash(&self, tx_hash: &Txid) -> Result<Value> {
        let blockhash = self.lookup_confirmed_blockhash(tx_hash, None)?;
        Ok(json!({ "block_hash": blockhash }))
    }

    pub fn get_headers(&self, heights: &[usize]) -> Vec<HeaderEntry> {
        let _timer = self
            .duration
            .with_label_values(&["get_headers"])
            .start_timer();
        let index = self.app.index();
        heights
            .iter()
            .filter_map(|height| index.get_header(*height))
            .collect()
    }

    pub fn get_best_header(&self) -> Result<HeaderEntry> {
        let last_header = self.app.index().best_header();
        Ok(last_header.chain_err(|| "no headers indexed")?)
    }

    pub fn get_merkle_proof(
        &self,
        tx_hash: &Txid,
        height: usize,
    ) -> Result<(Vec<TxMerkleNode>, usize)> {
        let header_entry = self
            .app
            .index()
            .get_header(height)
            .chain_err(|| format!("missing block #{}", height))?;
        let txids = self.app.daemon().getblocktxids(&header_entry.hash())?;
        let pos = txids
            .iter()
            .position(|txid| txid == tx_hash)
            .chain_err(|| format!("missing txid {}", tx_hash))?;
        let tx_nodes: Vec<TxMerkleNode> = txids
            .into_iter()
            .map(|txid| TxMerkleNode::from_inner(txid.into_inner()))
            .collect();
        let (branch, _root) = create_merkle_branch_and_root(tx_nodes, pos);
        Ok((branch, pos))
    }

    pub fn get_header_merkle_proof(
        &self,
        height: usize,
        cp_height: usize,
    ) -> Result<(Vec<Sha256dHash>, Sha256dHash)> {
        if cp_height < height {
            bail!("cp_height #{} < height #{}", cp_height, height);
        }

        let best_height = self.get_best_header()?.height();
        if best_height < cp_height {
            bail!(
                "cp_height #{} above best block height #{}",
                cp_height,
                best_height
            );
        }

        let heights: Vec<usize> = (0..=cp_height).collect();
        let header_hashes: Vec<BlockHash> = self
            .get_headers(&heights)
            .into_iter()
            .map(|h| *h.hash())
            .collect();
        let merkle_nodes: Vec<Sha256dHash> = header_hashes
            .iter()
            .map(|block_hash| Sha256dHash::from_inner(block_hash.into_inner()))
            .collect();
        assert_eq!(header_hashes.len(), heights.len());
        Ok(create_merkle_branch_and_root(merkle_nodes, height))
    }

    pub fn get_id_from_pos(
        &self,
        height: usize,
        tx_pos: usize,
        want_merkle: bool,
    ) -> Result<(Txid, Vec<TxMerkleNode>)> {
        let header_entry = self
            .app
            .index()
            .get_header(height)
            .chain_err(|| format!("missing block #{}", height))?;

        let txids = self.app.daemon().getblocktxids(header_entry.hash())?;
        let txid = *txids
            .get(tx_pos)
            .chain_err(|| format!("No tx in position #{} in block #{}", tx_pos, height))?;

        let tx_nodes = txids
            .into_iter()
            .map(|txid| TxMerkleNode::from_inner(txid.into_inner()))
            .collect();

        let branch = if want_merkle {
            create_merkle_branch_and_root(tx_nodes, tx_pos).0
        } else {
            vec![]
        };
        Ok((txid, branch))
    }

    pub fn broadcast(&self, txn: &Transaction) -> Result<Txid> {
        self.app.daemon().broadcast(txn)
    }

    pub fn update_mempool(&self) -> Result<()> {
        let _timer = self
            .duration
            .with_label_values(&["update_mempool"])
            .start_timer();
        self.tracker.write().unwrap().update(self.app.daemon())
    }

    /// Returns [vsize, fee_rate] pairs (measured in vbytes and satoshis).
    pub fn get_fee_histogram(&self) -> Vec<(f32, u32)> {
        self.tracker.read().unwrap().fee_histogram().clone()
    }

    // Fee rate [BTC/kB] to be confirmed in `blocks` from now.
    pub fn estimate_fee(&self, blocks: usize) -> f64 {
        let mut total_vsize = 0u32;
        let mut last_fee_rate = 0.0;
        let blocks_in_vbytes = (blocks * 1_000_000) as u32; // assume ~1MB blocks
        for (fee_rate, vsize) in self.tracker.read().unwrap().fee_histogram() {
            last_fee_rate = *fee_rate;
            total_vsize += vsize;
            if total_vsize >= blocks_in_vbytes {
                break; // under-estimate the fee rate a bit
            }
        }
        (last_fee_rate as f64) * 1e-5 // [BTC/kB] = 10^5 [sat/B]
    }

    pub fn get_banner(&self) -> Result<String> {
        self.app.get_banner()
    }

    pub fn get_relayfee(&self) -> Result<f64> {
        self.app.daemon().get_relayfee()
    }
}
