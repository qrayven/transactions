use anyhow::anyhow;
use crossbeam::channel::{Receiver, Sender};
use log::{debug, error, info, trace, warn};
use std::io;

use crate::models::{Client, ClientID, OperationRecord, OperationType};
use crate::payments::Payments;

const CSV_HEADER: &'static str = "client,available,held,total,locked";

pub fn marshaller(input: impl io::Read, output: crossbeam::channel::Sender<OperationRecord>) {
    info!("Starting the marshaller worker");

    let mut csv_reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(input);

    for result in csv_reader.deserialize::<OperationRecord>() {
        match result {
            Ok(record) => {
                trace!("Received operation record {:?}", record);
                if let Err(err) = output.send(record) {
                    error!("unable to send data from marshaller: {}", err)
                }
            }
            Err(err) => {
                warn!("unable to parse: {}. Ignoring", err);
            }
        }
    }
    debug!("Marshaller processor has been finished")
}

pub fn process_transactions(
    payments: &mut Payments,
    fan_in: Receiver<OperationRecord>,
    fan_out: Sender<(ClientID, Client)>,
) {
    info!("Starting payments processor");

    while let Ok(record) = fan_in.recv() {
        trace!(
            "{}: client: '{}', transaction: '{}'",
            record.operation,
            record.client_id,
            record.transaction_id
        );

        let result = match record.operation {
            OperationType::Chargeback => {
                payments.chargeback(record.client_id, record.transaction_id)
            }
            OperationType::Dispute => payments.dispute(record.client_id, record.transaction_id),
            OperationType::Resolve => payments.resolve(record.client_id, record.transaction_id),
            OperationType::Deposit => match record.amount {
                None => Err(anyhow!(
                    "the operation {} requires 'amount'",
                    record.operation
                )),
                Some(amount) => payments.deposit(record.client_id, record.transaction_id, amount),
            },
            OperationType::Withdrawal => match record.amount {
                None => Err(anyhow!(
                    "the operation {} requires 'amount'",
                    record.operation
                )),
                Some(amount) => payments.withdraw(record.client_id, record.transaction_id, amount),
            },
        };

        if let Err(err) = result {
            error!("{}", err);
        }
    }
    if let Err(err) = payments.send_logs(fan_out) {
        error!("{}", err);
    }
    debug!("Payments processor has been finished")
}

pub fn forwarder_to_stdout(rx: Receiver<(ClientID, Client)>) {
    println!("{}", CSV_HEADER);
    while let Ok((client_id, client)) = rx.recv() {
        println!("{},{}", client_id, client);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use std::fs;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Off)
            .try_init();
    }

    #[test]
    fn test_component() -> Result<()> {
        init();

        let file_input = fs::File::open("tests/input.csv")?;
        let expect = fs::read_to_string("tests/output.csv")?;

        let mut payments = Payments::new();
        let (tx, rx) = crossbeam::channel::bounded::<OperationRecord>(1024);
        let (log_tx, log_rx) = crossbeam::channel::bounded::<(ClientID, Client)>(1024);

        let marshaller_task = std::thread::spawn(move || marshaller(file_input, tx));
        let transactions_task =
            std::thread::spawn(move || process_transactions(&mut payments, rx, log_tx));
        marshaller_task.join().expect("");
        transactions_task.join().expect("");

        let mut collected = vec![];
        while let Ok(m) = log_rx.recv() {
            collected.push(m)
        }
        collected.sort_by(|a, b| a.0.cmp(&b.0));
        let content: String = collected
            .iter()
            .map(|(id, client)| format!("{},{}", id, client))
            .collect::<Vec<String>>()
            .join("\n");
        assert_eq!(expect, format!("{}\n{}\n", CSV_HEADER, content));
        Ok(())
    }
}
