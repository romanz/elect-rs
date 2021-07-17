use prometheus::{self, Encoder, IntGauge};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

pub use prometheus::{
    GaugeVec, Histogram, HistogramOpts, HistogramTimer, HistogramVec, IntCounter as Counter,
    IntCounterVec as CounterVec, IntGauge as Gauge, Opts as MetricOpts,
};

use crate::errors::*;
use crate::util::spawn_thread;

pub struct Metrics {
    reg: prometheus::Registry,
    addr: SocketAddr,
}

impl Metrics {
    pub fn new(addr: SocketAddr) -> Metrics {
        Metrics {
            reg: prometheus::Registry::new(),
            addr,
        }
    }

    pub fn counter(&self, opts: prometheus::Opts) -> Counter {
        let c = Counter::with_opts(opts).unwrap();
        self.reg.register(Box::new(c.clone())).unwrap();
        c
    }

    pub fn counter_vec(&self, opts: prometheus::Opts, labels: &[&str]) -> CounterVec {
        let c = CounterVec::new(opts, labels).unwrap();
        self.reg.register(Box::new(c.clone())).unwrap();
        c
    }

    pub fn gauge(&self, opts: prometheus::Opts) -> Gauge {
        let g = Gauge::with_opts(opts).unwrap();
        self.reg.register(Box::new(g.clone())).unwrap();
        g
    }

    pub fn gauge_vec(&self, opts: prometheus::Opts, labels: &[&str]) -> GaugeVec {
        let g = GaugeVec::new(opts, labels).unwrap();
        self.reg.register(Box::new(g.clone())).unwrap();
        g
    }

    pub fn gauge_int(&self, opts: prometheus::Opts) -> IntGauge {
        let g = Gauge::with_opts(opts).unwrap();
        self.reg.register(Box::new(g.clone())).unwrap();
        g
    }

    pub fn histogram(&self, opts: prometheus::HistogramOpts) -> Histogram {
        let h = Histogram::with_opts(opts).unwrap();
        self.reg.register(Box::new(h.clone())).unwrap();
        h
    }

    pub fn histogram_vec(&self, opts: prometheus::HistogramOpts, labels: &[&str]) -> HistogramVec {
        let h = HistogramVec::new(opts, labels).unwrap();
        self.reg.register(Box::new(h.clone())).unwrap();
        h
    }

    pub fn start(&self) {
        let server = tiny_http::Server::http(self.addr).unwrap_or_else(|e| {
            panic!(
                "failed to start monitoring HTTP server at {}: {}",
                self.addr, e
            )
        });
        start_process_exporter(&self);
        let reg = self.reg.clone();
        spawn_thread("metrics", move || loop {
            if let Err(e) = handle_request(&reg, server.recv()) {
                error!("http error: {}", e);
            }
        });
    }
}

fn handle_request(
    reg: &prometheus::Registry,
    request: io::Result<tiny_http::Request>,
) -> io::Result<()> {
    let request = request?;
    let mut buffer = vec![];
    prometheus::TextEncoder::new()
        .encode(&reg.gather(), &mut buffer)
        .unwrap();
    let response = tiny_http::Response::from_data(buffer);
    request.respond(response)
}

struct Stats {
    utime: f64,
    rss: u64,
    fds: usize,
}

#[cfg(feature="sysconf")]
fn ticks_per_second() -> Result<f64> {
    sysconf::raw::sysconf(sysconf::raw::SysconfVariable::ScClkTck)
        .map_err(|_| "failed to get _SC_CLK_TCK".into())
        .map(|x| x as f64)
}

#[cfg(not(feature="sysconf"))]
fn ticks_per_second() -> Result<f64> {
    Err("could not get _SC_CLK_TCK - not built with `--features sysconf`".into())
}

fn parse_stats() -> Result<Stats> {
    let value =
        fs::read_to_string("/proc/self/stat").chain_err(|| "failed to read /proc/self/stat")?;
    let parts: Vec<&str> = value.split_whitespace().collect();
    let page_size = page_size::get() as u64;
    let ticks_per_second = ticks_per_second()?;

    let parse_part = |index: usize, name: &str| -> Result<u64> {
        Ok(parts
            .get(index)
            .chain_err(|| format!("missing {}: {:?}", name, parts))?
            .parse::<u64>()
            .chain_err(|| format!("invalid {}: {:?}", name, parts))?)
    };

    // For details, see '/proc/[pid]/stat' section at `man 5 proc`:
    let utime = parse_part(13, "utime")? as f64 / ticks_per_second;
    let rss = parse_part(23, "rss")? * page_size;
    let fds = fs::read_dir("/proc/self/fd")
        .chain_err(|| "failed to read /proc/self/fd directory")?
        .count();
    Ok(Stats { utime, rss, fds })
}

fn start_process_exporter(metrics: &Metrics) {
    let rss = metrics.gauge(MetricOpts::new(
        "electrs_process_memory_rss",
        "Resident memory size [bytes]",
    ));
    let cpu = metrics.gauge_vec(
        MetricOpts::new(
            "electrs_process_cpu_usage",
            "CPU usage by this process [seconds]",
        ),
        &["type"],
    );
    let fds = metrics.gauge(MetricOpts::new(
        "electrs_process_open_fds",
        "# of file descriptors",
    ));
    spawn_thread("exporter", move || loop {
        match parse_stats() {
            Ok(stats) => {
                cpu.with_label_values(&["utime"]).set(stats.utime as f64);
                rss.set(stats.rss as i64);
                fds.set(stats.fds as i64);
            }
            Err(e) => {
                warn!("failed to export process stats: {}", e);
                return;
            }
        }
        thread::sleep(Duration::from_secs(5));
    });
}
