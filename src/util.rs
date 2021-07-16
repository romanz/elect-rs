use crate::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::iter::FromIterator;
use std::slice;
use std::thread;

pub type Bytes = Vec<u8>;
pub type HeaderMap = HashMap<BlockHash, BlockHeader>;

// TODO: consolidate serialization/deserialize code for bincode/bitcoin.
const HASH_LEN: usize = 32;
pub const HASH_PREFIX_LEN: usize = 8;

pub type FullHash = [u8; HASH_LEN];
pub type HashPrefix = [u8; HASH_PREFIX_LEN];

pub fn hash_prefix(hash: &[u8]) -> HashPrefix {
    hash[..HASH_PREFIX_LEN]
        .try_into()
        .expect("failed to convert into HashPrefix")
}

pub fn full_hash(hash: &[u8]) -> FullHash {
    hash.try_into().expect("failed to convert into FullHash")
}

#[derive(Eq, PartialEq, Clone)]
pub struct HeaderEntry {
    height: usize,
    hash: BlockHash,
    header: BlockHeader,
}

impl HeaderEntry {
    pub fn hash(&self) -> &BlockHash {
        &self.hash
    }

    pub fn header(&self) -> &BlockHeader {
        &self.header
    }

    pub fn height(&self) -> usize {
        self.height
    }
}

impl fmt::Debug for HeaderEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let spec = time::Timespec::new(i64::from(self.header().time), 0);
        let last_block_time = time::at_utc(spec).rfc3339().to_string();
        write!(
            f,
            "best={} height={} @ {}",
            self.hash(),
            self.height(),
            last_block_time,
        )
    }
}

struct HashedHeader {
    blockhash: BlockHash,
    header: BlockHeader,
}

fn hash_headers(headers: Vec<BlockHeader>) -> Vec<HashedHeader> {
    // header[i] -> header[i-1] (i.e. header.last() is the tip)
    let hashed_headers =
        Vec::<HashedHeader>::from_iter(headers.into_iter().map(|header| HashedHeader {
            blockhash: header.block_hash(),
            header,
        }));
    for i in 1..hashed_headers.len() {
        assert_eq!(
            hashed_headers[i].header.prev_blockhash,
            hashed_headers[i - 1].blockhash
        );
    }
    hashed_headers
}

pub struct HeaderList {
    headers: Vec<HeaderEntry>,
    heights: HashMap<BlockHash, usize>,
}

impl HeaderList {
    pub fn empty() -> HeaderList {
        HeaderList {
            headers: vec![],
            heights: HashMap::new(),
        }
    }

    pub fn order(&self, new_headers: Vec<BlockHeader>) -> Vec<HeaderEntry> {
        // header[i] -> header[i-1] (i.e. header.last() is the tip)
        let hashed_headers = hash_headers(new_headers);
        let prev_blockhash = match hashed_headers.first() {
            Some(h) => h.header.prev_blockhash,
            None => return vec![], // hashed_headers is empty
        };
        let null_hash = BlockHash::default();
        let new_height: usize = if prev_blockhash == null_hash {
            0
        } else {
            self.header_by_blockhash(&prev_blockhash)
                .unwrap_or_else(|| panic!("{} is not part of the blockchain", prev_blockhash))
                .height()
                + 1
        };
        (new_height..)
            .zip(hashed_headers.into_iter())
            .map(|(height, hashed_header)| HeaderEntry {
                height,
                hash: hashed_header.blockhash,
                header: hashed_header.header,
            })
            .collect()
    }

    pub fn apply(&mut self, new_headers: Vec<HeaderEntry>, tip: BlockHash) {
        if tip == BlockHash::default() {
            assert!(new_headers.is_empty());
            self.heights.clear();
            self.headers.clear();
            return;
        }
        // new_headers[i] -> new_headers[i - 1] (i.e. new_headers.last() is the tip)
        for i in 1..new_headers.len() {
            assert_eq!(new_headers[i - 1].height() + 1, new_headers[i].height());
            assert_eq!(
                *new_headers[i - 1].hash(),
                new_headers[i].header().prev_blockhash
            );
        }
        let new_height = match new_headers.first() {
            Some(entry) => {
                // Make sure tip is consistent (if there are new headers)
                let expected_tip = new_headers.last().unwrap().hash();
                assert_eq!(tip, *expected_tip);
                // Make sure first header connects correctly to existing chain
                let height = entry.height();
                let expected_prev_blockhash = if height > 0 {
                    *self.headers[height - 1].hash()
                } else {
                    BlockHash::default()
                };
                assert_eq!(entry.header().prev_blockhash, expected_prev_blockhash);
                // First new header's height (may override existing headers)
                height
            }
            // No new headers - chain's "tail" may be removed
            None => {
                let tip_height = *self
                    .heights
                    .get(&tip)
                    .unwrap_or_else(|| panic!("missing tip: {}", tip));
                tip_height + 1 // keep the tip, drop the rest
            }
        };
        debug!(
            "applying {} new headers from height {}",
            new_headers.len(),
            new_height
        );
        self.headers.truncate(new_height); // keep [0..new_height) entries
        assert_eq!(new_height, self.headers.len());
        for new_header in new_headers {
            assert_eq!(new_header.height(), self.headers.len());
            assert_eq!(new_header.header().prev_blockhash, self.tip());
            self.heights.insert(*new_header.hash(), new_header.height());
            self.headers.push(new_header);
        }
        assert_eq!(tip, self.tip());
        assert!(self.heights.contains_key(&tip));
    }

    pub fn header_by_blockhash(&self, blockhash: &BlockHash) -> Option<&HeaderEntry> {
        let height = self.heights.get(blockhash)?;
        let header = self.headers.get(*height)?;
        if *blockhash == *header.hash() {
            Some(header)
        } else {
            None
        }
    }

    pub fn header_by_height(&self, height: usize) -> Option<&HeaderEntry> {
        self.headers.get(height).map(|entry| {
            assert_eq!(entry.height(), height);
            entry
        })
    }

    pub fn equals(&self, other: &HeaderList) -> bool {
        self.headers.last() == other.headers.last()
    }

    pub fn tip(&self) -> BlockHash {
        self.headers.last().map(|h| *h.hash()).unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    pub fn iter(&self) -> slice::Iter<HeaderEntry> {
        self.headers.iter()
    }
}

pub struct SyncChannel<T> {
    tx: SyncSender<T>,
    rx: Receiver<T>,
}

impl<T> SyncChannel<T> {
    pub fn new(size: usize) -> SyncChannel<T> {
        let (tx, rx) = sync_channel(size);
        SyncChannel { tx, rx }
    }

    pub fn sender(&self) -> SyncSender<T> {
        self.tx.clone()
    }

    pub fn receiver(&self) -> &Receiver<T> {
        &self.rx
    }

    pub fn into_receiver(self) -> Receiver<T> {
        self.rx
    }
}

pub struct Channel<T> {
    tx: Sender<T>,
    rx: Receiver<T>,
}

impl<T> Channel<T> {
    pub fn unbounded() -> Self {
        let (tx, rx) = channel();
        Channel { tx, rx }
    }

    pub fn sender(&self) -> Sender<T> {
        self.tx.clone()
    }

    pub fn receiver(&self) -> &Receiver<T> {
        &self.rx
    }

    pub fn into_receiver(self) -> Receiver<T> {
        self.rx
    }
}

pub fn spawn_thread<F, T>(name: &str, f: F) -> thread::JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(f)
        .unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_headers() {
        use bitcoin::blockdata::block::BlockHeader;
        use bitcoin::hash_types::{BlockHash, TxMerkleNode};
        use bitcoin::hashes::Hash;

        use super::HeaderList;

        // Test an empty header list
        let null_hash = BlockHash::default();
        let mut header_list = HeaderList::empty();
        assert_eq!(header_list.tip(), null_hash);
        let ordered = header_list.order(vec![]);
        assert_eq!(ordered.len(), 0);
        header_list.apply(vec![], null_hash);

        let merkle_root = TxMerkleNode::hash(&[255]);
        let mut headers = vec![BlockHeader {
            version: 1,
            prev_blockhash: BlockHash::default(),
            merkle_root,
            time: 0,
            bits: 0,
            nonce: 0,
        }];
        for _height in 1..10 {
            let prev_blockhash = headers.last().unwrap().block_hash();
            let header = BlockHeader {
                version: 1,
                prev_blockhash,
                merkle_root,
                time: 0,
                bits: 0,
                nonce: 0,
            };
            headers.push(header);
        }

        // Test adding some new headers
        let ordered = header_list.order(headers[..3].to_vec());
        assert_eq!(ordered.len(), 3);
        header_list.apply(ordered.clone(), ordered[2].hash);
        assert_eq!(header_list.len(), 3);
        assert_eq!(header_list.tip(), ordered[2].hash);
        for h in 0..3 {
            let entry = header_list.header_by_height(h).unwrap();
            assert_eq!(entry.header, headers[h]);
            assert_eq!(entry.hash, headers[h].block_hash());
            assert_eq!(entry.height, h);
            assert_eq!(header_list.header_by_blockhash(&entry.hash), Some(entry));
        }

        // Test adding some more headers
        let ordered = header_list.order(headers[3..6].to_vec());
        assert_eq!(ordered.len(), 3);
        header_list.apply(ordered.clone(), ordered[2].hash);
        assert_eq!(header_list.len(), 6);
        assert_eq!(header_list.tip(), ordered[2].hash);
        for h in 0..6 {
            let entry = header_list.header_by_height(h).unwrap();
            assert_eq!(entry.header, headers[h]);
            assert_eq!(entry.hash, headers[h].block_hash());
            assert_eq!(entry.height, h);
            assert_eq!(header_list.header_by_blockhash(&entry.hash), Some(entry));
        }

        // Test adding some more headers (with an overlap)
        let ordered = header_list.order(headers[5..].to_vec());
        assert_eq!(ordered.len(), 5);
        header_list.apply(ordered.clone(), ordered[4].hash);
        assert_eq!(header_list.len(), 10);
        assert_eq!(header_list.tip(), ordered[4].hash);
        for h in 0..10 {
            let entry = header_list.header_by_height(h).unwrap();
            assert_eq!(entry.header, headers[h]);
            assert_eq!(entry.hash, headers[h].block_hash());
            assert_eq!(entry.height, h);
            assert_eq!(header_list.header_by_blockhash(&entry.hash), Some(entry));
        }

        // Reorg the chain and test apply() on it
        for h in 8..10 {
            headers[h].nonce += 1;
            headers[h].prev_blockhash = headers[h - 1].block_hash()
        }
        // Test reorging the chain
        let ordered = header_list.order(headers[8..10].to_vec());
        assert_eq!(ordered.len(), 2);
        header_list.apply(ordered.clone(), ordered[1].hash);
        assert_eq!(header_list.len(), 10);
        assert_eq!(header_list.tip(), ordered[1].hash);
        for h in 0..10 {
            let entry = header_list.header_by_height(h).unwrap();
            assert_eq!(entry.header, headers[h]);
            assert_eq!(entry.hash, headers[h].block_hash());
            assert_eq!(entry.height, h);
            assert_eq!(header_list.header_by_blockhash(&entry.hash), Some(entry));
        }

        // Test "trimming" the chain
        header_list.apply(vec![], headers[7].block_hash());
        assert_eq!(header_list.len(), 8);
        assert_eq!(header_list.tip(), headers[7].block_hash());
        for h in 0..8 {
            let entry = header_list.header_by_height(h).unwrap();
            assert_eq!(entry.header, headers[h]);
            assert_eq!(entry.hash, headers[h].block_hash());
            assert_eq!(entry.height, h);
            assert_eq!(header_list.header_by_blockhash(&entry.hash), Some(entry));
        }

        // Test "un-trimming" the chain
        let ordered = header_list.order(headers[8..].to_vec());
        assert_eq!(ordered.len(), 2);
        header_list.apply(ordered.clone(), ordered[1].hash);
        assert_eq!(header_list.len(), 10);
        assert_eq!(header_list.tip(), ordered[1].hash);
        for h in 0..10 {
            let entry = header_list.header_by_height(h).unwrap();
            assert_eq!(entry.header, headers[h]);
            assert_eq!(entry.hash, headers[h].block_hash());
            assert_eq!(entry.height, h);
            assert_eq!(header_list.header_by_blockhash(&entry.hash), Some(entry));
        }
    }
}
