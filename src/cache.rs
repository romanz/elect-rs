use crate::errors::*;
use crate::metrics::{CounterVec, MetricOpts, Metrics};

use crate::sync::Mutex;
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hash_types::{BlockHash, Txid};
use lru::LruCache;
use prometheus::IntGauge;
use std::hash::Hash;

struct SizedLruCache<K, V> {
    map: LruCache<K, (V, usize)>,
    bytes_usage: usize,
    bytes_capacity: usize,
    lookups: CounterVec,
    usage: IntGauge,
}

impl<K: Hash + Eq, V> SizedLruCache<K, V> {
    fn new(bytes_capacity: usize, lookups: CounterVec, usage: IntGauge) -> SizedLruCache<K, V> {
        SizedLruCache {
            map: LruCache::unbounded(),
            bytes_usage: 0,
            bytes_capacity,
            lookups,
            usage,
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        match self.map.get(key) {
            None => {
                self.lookups.with_label_values(&["miss"]).inc();
                None
            }
            Some((value, _)) => {
                self.lookups.with_label_values(&["hit"]).inc();
                Some(value)
            }
        }
    }

    fn put(&mut self, key: K, value: V, byte_size: usize) {
        if byte_size > self.bytes_capacity {
            return;
        }
        if let Some((_, popped_size)) = self.map.put(key, (value, byte_size)) {
            self.bytes_usage -= popped_size
        }
        self.bytes_usage += byte_size;

        while self.bytes_usage > self.bytes_capacity {
            match self.map.pop_lru() {
                Some((_, (_, popped_size))) => self.bytes_usage -= popped_size,
                None => break,
            }
        }

        self.usage.set(self.bytes_usage as i64);
    }
}

pub struct BlockTxIDsCache {
    map: Mutex<SizedLruCache<BlockHash, Vec<Txid>>>,
}

impl BlockTxIDsCache {
    pub fn new(bytes_capacity: usize, metrics: &Metrics) -> BlockTxIDsCache {
        let lookups = metrics.counter_vec(
            MetricOpts::new(
                "electrs_blocktxids_cache",
                "# of cache lookups for list of transactions in a block",
            ),
            &["type"],
        );
        let usage = metrics.gauge_int(MetricOpts::new(
            "electrs_blocktxids_cache_size",
            "Cache usage for list of transactions in a block (bytes)",
        ));
        BlockTxIDsCache {
            map: Mutex::new(SizedLruCache::new(bytes_capacity, lookups, usage)),
        }
    }

    pub fn get_or_else<F>(&self, blockhash: &BlockHash, load_txids_func: F) -> Result<Vec<Txid>>
    where
        F: FnOnce() -> Result<Vec<Txid>>,
    {
        if let Some(txids) = self.map.lock().unwrap().get(blockhash) {
            return Ok(txids.clone());
        }

        let txids = load_txids_func()?;
        let byte_size = 32 /* hash size */ * (1 /* key */ + txids.len() /* values */);
        self.map
            .lock()
            .unwrap()
            .put(*blockhash, txids.clone(), byte_size);
        Ok(txids)
    }
}

pub struct TransactionCache {
    // Store serialized transaction (should use less RAM).
    map: Mutex<SizedLruCache<Txid, Vec<u8>>>,
}

impl TransactionCache {
    pub fn new(bytes_capacity: usize, metrics: &Metrics) -> TransactionCache {
        let lookups = metrics.counter_vec(
            MetricOpts::new(
                "electrs_transactions_cache",
                "# of cache lookups for transactions",
            ),
            &["type"],
        );
        let usage = metrics.gauge_int(MetricOpts::new(
            "electrs_transactions_cache_size",
            "Cache usage for list of transactions (bytes)",
        ));
        TransactionCache {
            map: Mutex::new(SizedLruCache::new(bytes_capacity, lookups, usage)),
        }
    }

    pub fn get_or_else<F>(&self, txid: &Txid, load_txn_func: F) -> Result<Transaction>
    where
        F: FnOnce() -> Result<Vec<u8>>,
    {
        if let Some(serialized_txn) = self.map.lock().unwrap().get(txid) {
            return Ok(deserialize(&serialized_txn).chain_err(|| "failed to parse cached tx")?);
        }
        let serialized_txn = load_txn_func()?;
        let txn = deserialize(&serialized_txn).chain_err(|| "failed to parse serialized tx")?;
        let byte_size = 32 /* key (hash size) */ + serialized_txn.len();
        self.map
            .lock()
            .unwrap()
            .put(*txid, serialized_txn, byte_size);
        Ok(txn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    #[test]
    fn test_sized_lru_cache_hit_and_miss() {
        let counter = CounterVec::new(prometheus::Opts::new("name", "help"), &["type"]).unwrap();
        let usage = IntGauge::new("usage", "help").unwrap();
        let mut cache = SizedLruCache::<i8, i32>::new(100, counter.clone(), usage.clone());
        assert_eq!(counter.with_label_values(&["miss"]).get(), 0);
        assert_eq!(counter.with_label_values(&["hit"]).get(), 0);
        assert_eq!(usage.get(), 0);

        assert_eq!(cache.get(&1), None); // no such key
        assert_eq!(counter.with_label_values(&["miss"]).get(), 1);
        assert_eq!(counter.with_label_values(&["hit"]).get(), 0);
        assert_eq!(usage.get(), 0);

        cache.put(1, 10, 50); // add new key-value
        assert_eq!(cache.get(&1), Some(&10));
        assert_eq!(counter.with_label_values(&["miss"]).get(), 1);
        assert_eq!(counter.with_label_values(&["hit"]).get(), 1);
        assert_eq!(usage.get(), 50);

        cache.put(3, 30, 50); // drop oldest key (1)
        cache.put(2, 20, 50);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&20));
        assert_eq!(cache.get(&3), Some(&30));
        assert_eq!(counter.with_label_values(&["miss"]).get(), 2);
        assert_eq!(counter.with_label_values(&["hit"]).get(), 3);
        assert_eq!(usage.get(), 100);

        cache.put(3, 33, 50); // replace existing value
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&20));
        assert_eq!(cache.get(&3), Some(&33));
        assert_eq!(counter.with_label_values(&["miss"]).get(), 3);
        assert_eq!(counter.with_label_values(&["hit"]).get(), 5);
        assert_eq!(usage.get(), 100);

        cache.put(9, 90, 9999); // larger than cache capacity, don't drop the cache
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&20));
        assert_eq!(cache.get(&3), Some(&33));
        assert_eq!(cache.get(&9), None);
        assert_eq!(counter.with_label_values(&["miss"]).get(), 5);
        assert_eq!(counter.with_label_values(&["hit"]).get(), 7);
        assert_eq!(usage.get(), 100);
    }

    fn gen_hash<T: Hash>(seed: u8) -> T {
        let bytes: Vec<u8> = (seed..seed + 32).collect();
        <T as Hash>::hash(&bytes[..])
    }

    #[test]
    fn test_blocktxids_cache_hit_and_miss() {
        let block1: BlockHash = gen_hash(1);
        let block2: BlockHash = gen_hash(2);
        let block3: BlockHash = gen_hash(3);
        let txids: Vec<Txid> = vec![gen_hash(4), gen_hash(5)];

        let misses: Mutex<usize> = Mutex::new(0);
        let miss_func = || {
            *misses.lock().unwrap() += 1;
            Ok(txids.clone())
        };

        let dummy_metrics = Metrics::new("127.0.0.1:60000".parse().unwrap());
        // 200 bytes ~ 32 (bytes/hash) * (1 key hash + 2 value hashes) * 2 txns
        let cache = BlockTxIDsCache::new(200, &dummy_metrics);

        // cache miss
        let result = cache.get_or_else(&block1, &miss_func).unwrap();
        assert_eq!(1, *misses.lock().unwrap());
        assert_eq!(txids, result);

        // cache hit
        let result = cache.get_or_else(&block1, &miss_func).unwrap();
        assert_eq!(1, *misses.lock().unwrap());
        assert_eq!(txids, result);

        // cache size is 200, test that blockhash1 falls out of cache
        cache.get_or_else(&block2, &miss_func).unwrap();
        assert_eq!(2, *misses.lock().unwrap());
        cache.get_or_else(&block3, &miss_func).unwrap();
        assert_eq!(3, *misses.lock().unwrap());
        cache.get_or_else(&block1, &miss_func).unwrap();
        assert_eq!(4, *misses.lock().unwrap());

        // cache hits
        cache.get_or_else(&block3, &miss_func).unwrap();
        cache.get_or_else(&block1, &miss_func).unwrap();
        assert_eq!(4, *misses.lock().unwrap());
    }

    #[test]
    fn test_txn_cache() {
        use hex;

        let dummy_metrics = Metrics::new("127.0.0.1:60000".parse().unwrap());
        let cache = TransactionCache::new(1024, &dummy_metrics);
        let tx_bytes = hex::decode("0100000001a15d57094aa7a21a28cb20b59aab8fc7d1149a3bdbcddba9c622e4f5f6a99ece010000006c493046022100f93bb0e7d8db7bd46e40132d1f8242026e045f03a0efe71bbb8e3f475e970d790221009337cd7f1f929f00cc6ff01f03729b069a7c21b59b1736ddfee5db5946c5da8c0121033b9b137ee87d5a812d6f506efdd37f0affa7ffc310711c06c7f3e097c9447c52ffffffff0100e1f505000000001976a9140389035a9225b3839e2bbf32d826a1e222031fd888ac00000000").unwrap();

        let tx: Transaction = deserialize(&tx_bytes).unwrap();
        let txid = tx.txid();

        let mut misses = 0;
        assert_eq!(
            cache
                .get_or_else(&txid, || {
                    misses += 1;
                    Ok(tx_bytes.clone())
                })
                .unwrap(),
            tx
        );
        assert_eq!(misses, 1);
        assert_eq!(
            cache
                .get_or_else(&txid, || panic!("should not be called"))
                .unwrap(),
            tx
        );
        assert_eq!(misses, 1);
    }
}
