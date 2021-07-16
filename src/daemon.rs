use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use bitcoin::blockdata::block::{Block, BlockHeader};
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hash_types::{BlockHash, Txid};
use bitcoin::hashes::hex::{FromHex, ToHex};
use bitcoin::hashes::Hash;
use bitcoin::network::constants::Network;
use serde_json::{from_str, from_value, Map, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Lines, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use crate::cache::BlockTxIDsCache;
use crate::errors::*;
use crate::metrics::{HistogramOpts, HistogramVec, Metrics};
use crate::signal::Waiter;
use crate::util::HeaderList;

fn parse_hash<T: Hash>(value: &Value) -> Result<T> {
    Ok(T::from_hex(
        value
            .as_str()
            .chain_err(|| format!("non-string value: {}", value))?,
    )
    .chain_err(|| format!("non-hex value: {}", value))?)
}

fn header_from_value(value: Value) -> Result<BlockHeader> {
    let header_hex = value
        .as_str()
        .chain_err(|| format!("non-string header: {}", value))?;
    let header_bytes = hex::decode(header_hex).chain_err(|| "non-hex header")?;
    Ok(
        deserialize(&header_bytes)
            .chain_err(|| format!("failed to parse header {}", header_hex))?,
    )
}

fn block_from_value(value: Value) -> Result<Block> {
    let block_hex = value.as_str().chain_err(|| "non-string block")?;
    let block_bytes = hex::decode(block_hex).chain_err(|| "non-hex block")?;
    Ok(deserialize(&block_bytes).chain_err(|| format!("failed to parse block {}", block_hex))?)
}

fn tx_from_value(value: Value) -> Result<Transaction> {
    let tx_hex = value.as_str().chain_err(|| "non-string tx")?;
    let tx_bytes = hex::decode(tx_hex).chain_err(|| "non-hex tx")?;
    Ok(deserialize(&tx_bytes).chain_err(|| format!("failed to parse tx {}", tx_hex))?)
}

/// Parse JSONRPC error code, if exists.
fn parse_error_code(err: &Value) -> Option<i64> {
    if err.is_null() {
        return None;
    }
    err.as_object()?.get("code")?.as_i64()
}

fn check_error_code(reply_obj: &mut Map<String, Value>, method: &str) -> Result<()> {
    if let Some(err) = reply_obj.remove("error") {
        if let Some(code) = parse_error_code(&err) {
            match code {
                // RPC_IN_WARMUP -> retry by later reconnection
                -28 => bail!(ErrorKind::Connection(err.to_string())),
                _ => bail!(ErrorKind::Daemon(method.to_owned(), err)),
            }
        }
    }
    Ok(())
}

fn parse_jsonrpc_reply(mut reply: Value, method: &str, expected_id: u64) -> Result<Value> {
    if let Some(reply_obj) = reply.as_object_mut() {
        let id = reply_obj
            .get("id")
            .chain_err(|| format!("no id in reply: {:?}", reply_obj))?
            .clone();
        if id != expected_id {
            bail!(
                "wrong {} response id {}, expected {}",
                method,
                id,
                expected_id
            );
        }
        check_error_code(reply_obj, method)?;
        if let Some(result) = reply_obj.get_mut("result") {
            return Ok(result.take());
        }
        bail!("no result in reply: {:?}", reply_obj);
    }
    bail!("non-object reply: {:?}", reply);
}

#[derive(Serialize, Deserialize, Debug)]
struct BlockchainInfo {
    chain: String,
    blocks: u32,
    headers: u32,
    verificationprogress: f64,
    bestblockhash: String,
    pruned: bool,
    initialblockdownload: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct NetworkInfo {
    version: u64,
    subversion: String,
    relayfee: f64, // in BTC
}

pub struct MempoolEntry {
    fee: u64,   // in satoshis
    vsize: u32, // in virtual bytes (= weight/4)
    fee_per_vbyte: f32,
}

impl MempoolEntry {
    pub(crate) fn new(fee: u64, vsize: u32) -> MempoolEntry {
        MempoolEntry {
            fee,
            vsize,
            fee_per_vbyte: fee as f32 / vsize as f32,
        }
    }

    pub fn fee_per_vbyte(&self) -> f32 {
        self.fee_per_vbyte
    }

    pub fn fee(&self) -> u64 {
        self.fee
    }

    pub fn vsize(&self) -> u32 {
        self.vsize
    }
}

pub trait CookieGetter: Send + Sync {
    fn get(&self) -> Result<Vec<u8>>;
}

struct Connection {
    tx: TcpStream,
    rx: Lines<BufReader<TcpStream>>,
    cookie_getter: Arc<dyn CookieGetter>,
    addr: SocketAddr,
    signal: Waiter,
}

fn tcp_connect(addr: SocketAddr, signal: &Waiter) -> Result<TcpStream> {
    loop {
        match TcpStream::connect(addr) {
            Ok(conn) => return Ok(conn),
            Err(err) => {
                warn!("failed to connect daemon at {}: {}", addr, err);
                signal.wait(Duration::from_secs(3))?;
                continue;
            }
        }
    }
}

impl Connection {
    fn new(
        addr: SocketAddr,
        cookie_getter: Arc<dyn CookieGetter>,
        signal: Waiter,
    ) -> Result<Connection> {
        let conn = tcp_connect(addr, &signal)?;
        let reader = BufReader::new(
            conn.try_clone()
                .chain_err(|| format!("failed to clone {:?}", conn))?,
        );
        Ok(Connection {
            tx: conn,
            rx: reader.lines(),
            cookie_getter,
            addr,
            signal,
        })
    }

    fn reconnect(&self) -> Result<Connection> {
        Connection::new(self.addr, self.cookie_getter.clone(), self.signal.clone())
    }

    fn send(&mut self, request: &str) -> Result<()> {
        let cookie = &self.cookie_getter.get()?;
        let msg = format!(
            "POST / HTTP/1.1\nAuthorization: Basic {}\nContent-Length: {}\n\n{}",
            base64::encode(cookie),
            request.len(),
            request,
        );
        self.tx.write_all(msg.as_bytes()).chain_err(|| {
            ErrorKind::Connection("disconnected from daemon while sending".to_owned())
        })
    }

    fn recv(&mut self) -> Result<String> {
        // TODO: use proper HTTP parser.
        let mut in_header = true;
        let mut contents: Option<String> = None;
        let iter = self.rx.by_ref();
        let status = iter
            .next()
            .chain_err(|| {
                ErrorKind::Connection("disconnected from daemon while receiving".to_owned())
            })?
            .chain_err(|| "failed to read status")?;
        let mut headers = HashMap::new();
        for line in iter {
            let line = line.chain_err(|| ErrorKind::Connection("failed to read".to_owned()))?;
            if line.is_empty() {
                in_header = false; // next line should contain the actual response.
            } else if in_header {
                let parts: Vec<&str> = line.splitn(2, ": ").collect();
                if parts.len() == 2 {
                    headers.insert(parts[0].to_owned(), parts[1].to_owned());
                } else {
                    warn!("invalid header: {:?}", line);
                }
            } else {
                contents = Some(line);
                break;
            }
        }

        let contents =
            contents.chain_err(|| ErrorKind::Connection("no reply from daemon".to_owned()))?;
        let contents_length: &str = headers
            .get("Content-Length")
            .chain_err(|| format!("Content-Length is missing: {:?}", headers))?;
        let contents_length: usize = contents_length
            .parse()
            .chain_err(|| format!("invalid Content-Length: {:?}", contents_length))?;

        let expected_length = contents_length - 1; // trailing EOL is skipped
        if expected_length != contents.len() {
            bail!(ErrorKind::Connection(format!(
                "expected {} bytes, got {}",
                expected_length,
                contents.len()
            )));
        }

        Ok(if status == "HTTP/1.1 200 OK" {
            contents
        } else if status == "HTTP/1.1 500 Internal Server Error" {
            warn!("HTTP status: {}", status);
            contents // the contents should have a JSONRPC error field
        } else {
            bail!(
                "request failed {:?}: {:?} = {:?}",
                status,
                headers,
                contents
            );
        })
    }
}

struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn new() -> Self {
        Counter { value: 0.into() }
    }

    fn next(&self) -> u64 {
        // fetch_add() returns previous value, we want current one
        self.value.fetch_add(1, Ordering::Relaxed) + 1
    }
}

pub struct Daemon {
    daemon_dir: PathBuf,
    blocks_dir: PathBuf,
    network: Network,
    conn: Mutex<Connection>,
    message_id: Counter, // for monotonic JSONRPC 'id'
    signal: Waiter,
    blocktxids_cache: Arc<BlockTxIDsCache>,

    // monitoring
    latency: HistogramVec,
    size: HistogramVec,
}

impl Daemon {
    pub fn new(
        daemon_dir: &PathBuf,
        blocks_dir: &PathBuf,
        daemon_rpc_addr: SocketAddr,
        cookie_getter: Arc<dyn CookieGetter>,
        network: Network,
        signal: Waiter,
        blocktxids_cache: Arc<BlockTxIDsCache>,
        metrics: &Metrics,
    ) -> Result<Daemon> {
        let daemon = Daemon {
            daemon_dir: daemon_dir.clone(),
            blocks_dir: blocks_dir.clone(),
            network,
            conn: Mutex::new(Connection::new(
                daemon_rpc_addr,
                cookie_getter,
                signal.clone(),
            )?),
            message_id: Counter::new(),
            blocktxids_cache,
            signal: signal.clone(),
            latency: metrics.histogram_vec(
                HistogramOpts::new("electrs_daemon_rpc", "Bitcoind RPC latency (in seconds)"),
                &["method"],
            ),
            // TODO: use better buckets (e.g. 1 byte to 10MB).
            size: metrics.histogram_vec(
                HistogramOpts::new("electrs_daemon_bytes", "Bitcoind RPC size (in bytes)"),
                &["method", "dir"],
            ),
        };
        let network_info = daemon.getnetworkinfo()?;
        info!("{:?}", network_info);
        if network_info.version < 16_00_00 {
            bail!(
                "{} is not supported - please use bitcoind 0.16+",
                network_info.subversion,
            )
        }
        let blockchain_info = daemon.getblockchaininfo()?;
        info!("{:?}", blockchain_info);
        if blockchain_info.pruned {
            bail!("pruned node is not supported (use '-prune=0' bitcoind flag)".to_owned())
        }
        loop {
            let info = daemon.getblockchaininfo()?;
            if !info.initialblockdownload {
                break;
            }
            if network == Network::Regtest && info.headers == info.blocks {
                break;
            }
            warn!(
                "wait until IBD is over: headers={} blocks={} progress={}",
                info.headers, info.blocks, info.verificationprogress
            );
            signal.wait(Duration::from_secs(3))?;
        }
        Ok(daemon)
    }

    pub fn reconnect(&self) -> Result<Daemon> {
        Ok(Daemon {
            daemon_dir: self.daemon_dir.clone(),
            blocks_dir: self.blocks_dir.clone(),
            network: self.network,
            conn: Mutex::new(self.conn.lock().unwrap().reconnect()?),
            message_id: Counter::new(),
            signal: self.signal.clone(),
            blocktxids_cache: Arc::clone(&self.blocktxids_cache),
            latency: self.latency.clone(),
            size: self.size.clone(),
        })
    }

    pub fn list_blk_files(&self) -> Result<Vec<PathBuf>> {
        let path = self.blocks_dir.join("blk*.dat");
        info!("listing block files at {:?}", path);
        let mut paths: Vec<PathBuf> = glob::glob(path.to_str().unwrap())
            .chain_err(|| "failed to list blk*.dat files")?
            .map(std::result::Result::unwrap)
            .collect();
        paths.sort();
        Ok(paths)
    }

    pub fn magic(&self) -> u32 {
        self.network.magic()
    }

    fn call_jsonrpc(&self, method: &str, request: &Value) -> Result<Value> {
        let mut conn = self.conn.lock().unwrap();
        let timer = self.latency.with_label_values(&[method]).start_timer();
        let request = request.to_string();
        conn.send(&request)?;
        self.size
            .with_label_values(&[method, "send"])
            .observe(request.len() as f64);
        let response = conn.recv()?;
        let result: Value = from_str(&response).chain_err(|| "invalid JSON")?;
        timer.observe_duration();
        self.size
            .with_label_values(&[method, "recv"])
            .observe(response.len() as f64);
        Ok(result)
    }

    fn handle_request_batch(&self, method: &str, params_list: &[Value]) -> Result<Vec<Value>> {
        let id = self.message_id.next();
        let reqs = params_list
            .iter()
            .map(|params| json!({"method": method, "params": params, "id": id}))
            .collect();
        let mut results = vec![];
        let mut replies = self.call_jsonrpc(method, &reqs)?;
        if let Some(replies_vec) = replies.as_array_mut() {
            for reply in replies_vec {
                results.push(parse_jsonrpc_reply(reply.take(), method, id)?)
            }
            return Ok(results);
        }
        bail!("non-array replies: {:?}", replies);
    }

    fn retry_request_batch(&self, method: &str, params_list: &[Value]) -> Result<Vec<Value>> {
        loop {
            match self.handle_request_batch(method, params_list) {
                Err(Error(ErrorKind::Connection(msg), _)) => {
                    warn!("reconnecting to bitcoind: {}", msg);
                    self.signal.wait(Duration::from_secs(3))?;
                    let mut conn = self.conn.lock().unwrap();
                    *conn = conn.reconnect()?;
                    continue;
                }
                result => return result,
            }
        }
    }

    fn request(&self, method: &str, params: Value) -> Result<Value> {
        let mut values = self.retry_request_batch(method, &[params])?;
        assert_eq!(values.len(), 1);
        Ok(values.remove(0))
    }

    fn requests(&self, method: &str, params_list: &[Value]) -> Result<Vec<Value>> {
        self.retry_request_batch(method, params_list)
    }

    // bitcoind JSONRPC API:

    fn getblockchaininfo(&self) -> Result<BlockchainInfo> {
        let info: Value = self.request("getblockchaininfo", json!([]))?;
        Ok(from_value(info).chain_err(|| "invalid blockchain info")?)
    }

    fn getnetworkinfo(&self) -> Result<NetworkInfo> {
        let info: Value = self.request("getnetworkinfo", json!([]))?;
        Ok(from_value(info).chain_err(|| "invalid network info")?)
    }

    pub fn get_subversion(&self) -> Result<String> {
        Ok(self.getnetworkinfo()?.subversion)
    }

    pub fn get_relayfee(&self) -> Result<f64> {
        Ok(self.getnetworkinfo()?.relayfee)
    }

    pub fn getbestblockhash(&self) -> Result<BlockHash> {
        parse_hash(&self.request("getbestblockhash", json!([]))?).chain_err(|| "invalid blockhash")
    }

    pub fn getblockheader(&self, blockhash: &BlockHash) -> Result<BlockHeader> {
        header_from_value(self.request(
            "getblockheader",
            json!([blockhash.to_hex(), /*verbose=*/ false]),
        )?)
    }

    pub fn getblockheaders(&self, heights: &[usize]) -> Result<Vec<BlockHeader>> {
        let heights: Vec<Value> = heights.iter().map(|height| json!([height])).collect();
        let params_list: Vec<Value> = self
            .requests("getblockhash", &heights)?
            .into_iter()
            .map(|hash| json!([hash, /*verbose=*/ false]))
            .collect();
        let mut result = vec![];
        for h in self.requests("getblockheader", &params_list)? {
            result.push(header_from_value(h)?);
        }
        Ok(result)
    }

    pub fn getblock(&self, blockhash: &BlockHash) -> Result<Block> {
        let block = block_from_value(
            self.request("getblock", json!([blockhash.to_hex(), /*verbose=*/ false]))?,
        )?;
        assert_eq!(block.block_hash(), *blockhash);
        Ok(block)
    }

    fn load_blocktxids(&self, blockhash: &BlockHash) -> Result<Vec<Txid>> {
        self.request("getblock", json!([blockhash.to_hex(), /*verbose=*/ 1]))?
            .get("tx")
            .chain_err(|| "block missing txids")?
            .as_array()
            .chain_err(|| "invalid block txids")?
            .iter()
            .map(parse_hash)
            .collect::<Result<Vec<Txid>>>()
    }

    pub fn getblocktxids(&self, blockhash: &BlockHash) -> Result<Vec<Txid>> {
        self.blocktxids_cache
            .get_or_else(&blockhash, || self.load_blocktxids(blockhash))
    }

    pub fn gettransaction(
        &self,
        txhash: &Txid,
        blockhash: Option<BlockHash>,
    ) -> Result<Transaction> {
        let mut args = json!([txhash.to_hex(), /*verbose=*/ false]);
        if let Some(blockhash) = blockhash {
            args.as_array_mut().unwrap().push(json!(blockhash.to_hex()));
        }
        tx_from_value(self.request("getrawtransaction", args)?)
    }

    pub fn gettransaction_raw(
        &self,
        txhash: &Txid,
        blockhash: Option<BlockHash>,
        verbose: bool,
    ) -> Result<Value> {
        let mut args = json!([txhash.to_hex(), verbose]);
        if let Some(blockhash) = blockhash {
            args.as_array_mut().unwrap().push(json!(blockhash.to_hex()));
        }
        Ok(self.request("getrawtransaction", args)?)
    }

    pub fn getmempooltxids(&self) -> Result<HashSet<Txid>> {
        let txids: Value = self.request("getrawmempool", json!([/*verbose=*/ false]))?;
        let mut result = HashSet::new();
        for value in txids.as_array().chain_err(|| "non-array result")? {
            result.insert(parse_hash(&value).chain_err(|| "invalid txid")?);
        }
        Ok(result)
    }

    pub fn getmempoolentry(&self, txid: &Txid) -> Result<MempoolEntry> {
        let entry = self.request("getmempoolentry", json!([txid.to_hex()]))?;
        let fee = (entry
            .get("fee")
            .chain_err(|| "missing fee")?
            .as_f64()
            .chain_err(|| "non-float fee")?
            * 100_000_000f64) as u64;
        let vsize = entry
            .get("size")
            .or_else(|| entry.get("vsize")) // (https://github.com/bitcoin/bitcoin/pull/15637)
            .chain_err(|| "missing vsize")?
            .as_u64()
            .chain_err(|| "non-integer vsize")? as u32;
        Ok(MempoolEntry::new(fee, vsize))
    }

    pub fn broadcast(&self, tx: &Transaction) -> Result<Txid> {
        let tx = hex::encode(serialize(tx));
        let txid = self.request("sendrawtransaction", json!([tx]))?;
        Ok(
            Txid::from_hex(txid.as_str().chain_err(|| "non-string txid")?)
                .chain_err(|| "failed to parse txid")?,
        )
    }

    fn get_all_headers(&self, tip: &BlockHash) -> Result<Vec<BlockHeader>> {
        let info: Value = self.request("getblockheader", json!([tip.to_hex()]))?;
        let tip_height = info
            .get("height")
            .expect("missing height")
            .as_u64()
            .expect("non-numeric height") as usize;
        let all_heights: Vec<usize> = (0..=tip_height).collect();
        let chunk_size = 100_000;
        let mut result = vec![];
        let null_hash = BlockHash::default();
        for heights in all_heights.chunks(chunk_size) {
            trace!("downloading {} block headers", heights.len());
            let mut headers = self.getblockheaders(&heights)?;
            assert!(headers.len() == heights.len());
            result.append(&mut headers);
        }

        let mut blockhash = null_hash;
        for header in &result {
            assert_eq!(header.prev_blockhash, blockhash);
            blockhash = header.block_hash();
        }
        assert_eq!(blockhash, *tip);
        Ok(result)
    }

    // Returns a list of BlockHeaders in ascending height (i.e. the tip is last).
    pub fn get_new_headers(
        &self,
        indexed_headers: &HeaderList,
        bestblockhash: &BlockHash,
    ) -> Result<Vec<BlockHeader>> {
        // Iterate back over headers until known blockash is found:
        if indexed_headers.is_empty() {
            return self.get_all_headers(bestblockhash);
        }
        debug!(
            "downloading new block headers ({} already indexed) from {}",
            indexed_headers.len(),
            bestblockhash,
        );
        let mut new_headers = vec![];
        let null_hash = BlockHash::default();
        let mut blockhash = *bestblockhash;
        while blockhash != null_hash {
            if indexed_headers.header_by_blockhash(&blockhash).is_some() {
                break;
            }
            let header = self
                .getblockheader(&blockhash)
                .chain_err(|| format!("failed to get {} header", blockhash))?;
            new_headers.push(header);
            blockhash = header.prev_blockhash;
        }
        trace!("downloaded {} block headers", new_headers.len());
        new_headers.reverse(); // so the tip is the last vector entry
        Ok(new_headers)
    }
}
