use crate::sync::{
    mpsc::{Receiver, SyncSender},
    Arc, Mutex,
};
use bitcoin::blockdata::block::Block;
use bitcoin::consensus::encode::{deserialize, Decodable};
use bitcoin::hash_types::BlockHash;
use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::thread;

use crate::daemon::Daemon;
use crate::errors::*;
use crate::index::{index_block, last_indexed_block, read_indexed_blockhashes};
use crate::metrics::{CounterVec, Histogram, HistogramOpts, HistogramVec, MetricOpts, Metrics};
use crate::signal::Waiter;
use crate::store::{DBStore, Row, WriteStore};
use crate::util::{spawn_thread, HeaderList, SyncChannel};

struct Parser {
    magic: u32,
    current_headers: HeaderList,
    indexed_blockhashes: Mutex<HashSet<BlockHash>>,
    // metrics
    duration: HistogramVec,
    block_count: CounterVec,
    bytes_read: Histogram,
}

impl Parser {
    fn new(
        daemon: &Daemon,
        metrics: &Metrics,
        indexed_blockhashes: HashSet<BlockHash>,
    ) -> Result<Arc<Parser>> {
        Ok(Arc::new(Parser {
            magic: daemon.magic(),
            current_headers: load_headers(daemon)?,
            indexed_blockhashes: Mutex::new(indexed_blockhashes),
            duration: metrics.histogram_vec(
                HistogramOpts::new(
                    "electrs_parse_duration",
                    "blk*.dat parsing duration (in seconds)",
                ),
                &["step"],
            ),
            block_count: metrics.counter_vec(
                MetricOpts::new("electrs_parse_blocks", "# of block parsed (from blk*.dat)"),
                &["type"],
            ),

            bytes_read: metrics.histogram(HistogramOpts::new(
                "electrs_parse_bytes_read",
                "# of bytes read (from blk*.dat)",
            )),
        }))
    }

    fn last_indexed_row(&self) -> Row {
        // TODO: use JSONRPC for missing blocks, and don't use 'L' row at all.
        let indexed_blockhashes = self.indexed_blockhashes.lock().unwrap();
        let last_header = self
            .current_headers
            .iter()
            .take_while(|h| indexed_blockhashes.contains(h.hash()))
            .last()
            .expect("no indexed header found");
        debug!("last indexed block: {:?}", last_header);
        last_indexed_block(last_header.hash())
    }

    fn read_blkfile(&self, path: &Path) -> Result<Vec<u8>> {
        let timer = self.duration.with_label_values(&["read"]).start_timer();
        let blob = fs::read(&path).chain_err(|| format!("failed to read {:?}", path))?;
        timer.observe_duration();
        self.bytes_read.observe(blob.len() as f64);
        Ok(blob)
    }

    fn index_blkfile(&self, blob: Vec<u8>) -> Result<Vec<Row>> {
        let timer = self.duration.with_label_values(&["parse"]).start_timer();
        let blocks = parse_blocks(blob, self.magic)?;
        timer.observe_duration();

        let mut rows = Vec::<Row>::new();
        let timer = self.duration.with_label_values(&["index"]).start_timer();
        for block in blocks {
            let blockhash = block.block_hash();
            if let Some(header) = self.current_headers.header_by_blockhash(&blockhash) {
                if self
                    .indexed_blockhashes
                    .lock()
                    .expect("indexed_blockhashes")
                    .insert(blockhash)
                {
                    rows.extend(index_block(&block, header.height()));
                    self.block_count.with_label_values(&["indexed"]).inc();
                } else {
                    self.block_count.with_label_values(&["duplicate"]).inc();
                }
            } else {
                // will be indexed later (after bulk load is over) if not an orphan block
                self.block_count.with_label_values(&["skipped"]).inc();
            }
        }
        timer.observe_duration();

        let timer = self.duration.with_label_values(&["sort"]).start_timer();
        rows.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        timer.observe_duration();
        Ok(rows)
    }
}

fn parse_blocks(blob: Vec<u8>, magic: u32) -> Result<Vec<Block>> {
    let mut cursor = Cursor::new(&blob);
    let mut blocks = vec![];
    let max_pos = blob.len() as u64;
    while cursor.position() < max_pos {
        let offset = cursor.position();
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic != value {
                    cursor.set_position(offset + 1);
                    continue;
                }
            }
            Err(_) => break, // EOF
        };
        let block_size = u32::consensus_decode(&mut cursor).chain_err(|| "no block size")?;
        let start = cursor.position();
        let end = start + block_size as u64;

        // If Core's WriteBlockToDisk ftell fails, only the magic bytes and size will be written
        // and the block body won't be written to the blk*.dat file.
        // Since the first 4 bytes should contain the block's version, we can skip such blocks
        // by peeking the cursor (and skipping previous `magic` and `block_size`).
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic == value {
                    cursor.set_position(start);
                    continue;
                }
            }
            Err(_) => break, // EOF
        }
        let block: Block = deserialize(&blob[start as usize..end as usize])
            .chain_err(|| format!("failed to parse block at {}..{}", start, end))?;
        blocks.push(block);
        cursor.set_position(end as u64);
    }
    Ok(blocks)
}

fn load_headers(daemon: &Daemon) -> Result<HeaderList> {
    let tip = daemon.getbestblockhash()?;
    let mut headers = HeaderList::empty();
    let new_headers = headers.order(daemon.get_new_headers(&headers, &tip)?);
    headers.apply(new_headers, tip);
    Ok(headers)
}

fn set_open_files_limit(limit: libc::rlim_t) {
    let resource = libc::RLIMIT_NOFILE;
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let result = unsafe { libc::getrlimit(resource, &mut rlim) };
    if result < 0 {
        panic!("getrlimit() failed: {}", result);
    }
    rlim.rlim_cur = limit; // set softs limit only.
    let result = unsafe { libc::setrlimit(resource, &rlim) };
    if result < 0 {
        panic!("setrlimit() failed: {}", result);
    }
}

type JoinHandle = thread::JoinHandle<Result<()>>;
type BlobReceiver = Arc<Mutex<Receiver<(Vec<u8>, PathBuf)>>>;

fn start_reader(blk_files: Vec<PathBuf>, parser: Arc<Parser>) -> (BlobReceiver, JoinHandle) {
    let chan = SyncChannel::new(0);
    let blobs = chan.sender();
    let handle = spawn_thread("bulk_read", move || -> Result<()> {
        for path in blk_files {
            blobs
                .send((parser.read_blkfile(&path)?, path))
                .expect("failed to send blk*.dat contents");
        }
        Ok(())
    });
    (Arc::new(Mutex::new(chan.into_receiver())), handle)
}

fn start_indexer(
    blobs: BlobReceiver,
    parser: Arc<Parser>,
    writer: SyncSender<(Vec<Row>, PathBuf)>,
) -> JoinHandle {
    spawn_thread("bulk_index", move || -> Result<()> {
        loop {
            let msg = blobs.lock().unwrap().recv();
            if let Ok((blob, path)) = msg {
                let rows = parser
                    .index_blkfile(blob)
                    .chain_err(|| format!("failed to index {:?}", path))?;
                writer
                    .send((rows, path))
                    .expect("failed to send indexed rows")
            } else {
                debug!("no more blocks to index");
                break;
            }
        }
        Ok(())
    })
}

pub fn index_blk_files(
    daemon: &Daemon,
    index_threads: usize,
    metrics: &Metrics,
    signal: &Waiter,
    store: DBStore,
) -> Result<DBStore> {
    set_open_files_limit(2048); // twice the default `ulimit -n` value
    let blk_files = daemon.list_blk_files()?;
    info!("indexing {} blk*.dat files", blk_files.len());
    let indexed_blockhashes = read_indexed_blockhashes(&store);
    debug!("found {} indexed blocks", indexed_blockhashes.len());
    let parser = Parser::new(daemon, metrics, indexed_blockhashes)?;
    let (blobs, reader) = start_reader(blk_files, parser.clone());
    let rows_chan = SyncChannel::new(0);
    let indexers: Vec<JoinHandle> = (0..index_threads)
        .map(|_| start_indexer(blobs.clone(), parser.clone(), rows_chan.sender()))
        .collect();

    for (rows, path) in rows_chan.into_receiver() {
        trace!("indexed {:?}: {} rows", path, rows.len());
        store.write(rows);
        signal
            .poll()
            .chain_err(|| "stopping bulk indexing due to signal")?;
    }
    reader
        .join()
        .expect("reader panicked")
        .expect("reader failed");

    indexers.into_iter().for_each(|i| {
        i.join()
            .expect("indexer panicked")
            .expect("indexing failed")
    });
    store.write(vec![parser.last_indexed_row()]);
    Ok(store)
}

#[cfg(test)]
mod tests {

    use super::*;
    use bitcoin::hashes::Hash;
    use hex::decode as hex_decode;

    #[test]
    fn test_incomplete_block_parsing() {
        let magic = 0x0709110b;
        let raw_blocks = hex_decode(fixture("incomplete_block.hex")).unwrap();
        let blocks = parse_blocks(raw_blocks, magic).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(
            blocks[1].block_hash().into_inner().to_vec(),
            hex_decode("d55acd552414cc44a761e8d6b64a4d555975e208397281d115336fc500000000").unwrap()
        );
    }

    pub fn fixture(filename: &str) -> String {
        let path = Path::new("src")
            .join("tests")
            .join("fixtures")
            .join(filename);
        fs::read_to_string(path).unwrap()
    }
}
