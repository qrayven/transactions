use anyhow::{anyhow, bail, Result};
use log::debug;
use models::Client;
use models::ClientID;
use models::OperationRecord;
use payments::Payments;
use std::env;
use std::io;

mod models;
mod payments;
mod processors;

fn main() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Off)
        .try_init();

    let file_input = get_input_file().expect("error while getting the input of the file");
    let (tx, rx) = crossbeam::channel::bounded::<OperationRecord>(1024);
    let (log_tx, log_rx) = crossbeam::channel::bounded::<(ClientID, Client)>(1024);

    let mut payments = Payments::new();
    let marshaller_task = std::thread::spawn(move || processors::marshaller(file_input, tx));
    let payments_task =
        std::thread::spawn(move || processors::process_transactions(&mut payments, rx, log_tx));
    let stdout_task = std::thread::spawn(move || processors::forwarder_to_stdout(log_rx));

    marshaller_task
        .join()
        .expect("failed to run marshaller task");
    payments_task.join().expect("failed to run payments task");
    stdout_task.join().expect("failed to run stdout task");
}

fn get_input_file() -> Result<impl io::Read> {
    let filepath: String = match env::args().nth(1) {
        Some(f) => f,
        None => bail!("filename is not give"),
    };

    debug!("Getting input data from file {}", filepath);
    std::fs::File::open(filepath).map_err(|e| anyhow!("{}", e))
}
