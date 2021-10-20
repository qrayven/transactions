use crate::models::{Client, ClientID, OperationType, Transaction, TransactionID};
use anyhow::{bail, Result as AnyResult};
use crossbeam::channel::Sender;
use fnv::FnvHasher;
use log::debug;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use thiserror::Error;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

#[derive(Error, Debug)]
pub enum PaymentsError {
    #[error("Insufficient balance")]
    InsufficientBalance,
    #[error("The '{0}' has to high precision")]
    AmountPrecisionToHigh(Decimal),
    #[error("The '{0}' is negative")]
    AmountNegative(Decimal),
    #[error("The client '{0}' doesn't exist")]
    ClientNotExist(u16),
    #[error("The transaction '{0}' already exist")]
    TransactionAlreadyExists(u32),
    #[error("The transaction '{0}' has an invalid state: '{1}'")]
    TransactionInvalidState(u32, OperationType),
    #[error("The transaction '{0}' doesn't exist or you don't have permissions")]
    TransactionNotExists(u32),
    #[error("The account '{0}' id locked")]
    AccountLocked(u16),
}

#[derive(Default)]
pub struct Payments {
    // as the transaction ID is global, we must to use global store for transactions
    // go guarantee the uniqueness of transaction
    pub transactions: HashMapFnv<TransactionID, Transaction>,
    pub clients: HashMapFnv<ClientID, Client>,
}

impl Payments {
    pub fn new() -> Self {
        Payments::default()
    }

    pub fn deposit(
        &mut self,
        client_id: ClientID,
        transaction_id: TransactionID,
        amount: Decimal,
    ) -> AnyResult<()> {
        Self::validate_amount(amount)?;

        let transaction_vacant = match self.transactions.entry(transaction_id) {
            Entry::Occupied(_) => {
                bail!(PaymentsError::TransactionAlreadyExists(transaction_id));
            }
            Entry::Vacant(v) => v,
        };

        match self.clients.entry(client_id) {
            Entry::Occupied(mut c) => {
                let client = c.get_mut();
                if client.locked {
                    bail!(PaymentsError::AccountLocked(client_id));
                }
                client.available += amount;
                client.total += amount;
            }
            Entry::Vacant(v) => {
                debug!(
                    "the client with id '{}' doesn't exists - creating a new one",
                    client_id
                );
                v.insert(Client {
                    available: amount,
                    total: amount,
                    ..Default::default()
                });
            }
        };
        // we don't want to insert the transaction until the account-related conditions (locking) are met
        transaction_vacant.insert(Transaction {
            client_id,
            amount,
            last_operation: OperationType::Deposit,
        });

        Ok(())
    }

    pub fn withdraw(
        &mut self,
        client_id: ClientID,
        transaction_id: TransactionID,
        amount: Decimal,
    ) -> AnyResult<()> {
        Self::validate_amount(amount)?;

        let client = Self::obtain_client(&mut self.clients, client_id)?;
        if client.available < amount {
            bail!(PaymentsError::InsufficientBalance)
        }

        // before applying the changes to the account, we must make sure the transaction is unique
        match self.transactions.entry(transaction_id) {
            Entry::Occupied(_) => {
                bail!(PaymentsError::TransactionAlreadyExists(transaction_id))
            }
            Entry::Vacant(v) => v.insert(Transaction {
                client_id,
                amount,
                last_operation: OperationType::Withdrawal,
            }),
        };

        client.available -= amount;
        client.total -= amount;

        Ok(())
    }

    pub fn dispute(&mut self, client_id: ClientID, transaction_id: TransactionID) -> AnyResult<()> {
        let transaction =
            Self::obtain_transaction(&mut self.transactions, transaction_id, client_id)?;
        let client = Self::obtain_client(&mut self.clients, client_id)?;

        if !transaction.last_operation.is_disputable() {
            bail!(
                "the transaction cannot be disputed, {}",
                PaymentsError::TransactionInvalidState(transaction_id, transaction.last_operation)
            )
        }

        client.available -= transaction.amount;
        client.held += transaction.amount;
        transaction.last_operation = OperationType::Dispute;

        Ok(())
    }

    pub fn resolve(&mut self, client_id: ClientID, transaction_id: TransactionID) -> AnyResult<()> {
        let transaction =
            Self::obtain_transaction(&mut self.transactions, transaction_id, client_id)?;
        if transaction.last_operation != OperationType::Dispute {
            bail!(
                "the transaction cannot be resolved. It is not disputed, {}",
                PaymentsError::TransactionInvalidState(transaction_id, transaction.last_operation)
            )
        }

        let client = Self::obtain_client(&mut self.clients, client_id)?;
        client.held -= transaction.amount;
        client.available += transaction.amount;
        transaction.last_operation = OperationType::Resolve;

        Ok(())
    }

    pub fn chargeback(
        &mut self,
        client_id: ClientID,
        transaction_id: TransactionID,
    ) -> AnyResult<()> {
        let transaction =
            Self::obtain_transaction(&mut self.transactions, transaction_id, client_id)?;

        if transaction.last_operation != OperationType::Dispute {
            bail!(
                "the transaction cannot be charged-back. It is not disputed, {}",
                PaymentsError::TransactionInvalidState(transaction_id, transaction.last_operation)
            )
        }

        let client = Self::obtain_client(&mut self.clients, client_id)?;
        client.held -= transaction.amount;
        client.total -= transaction.amount;
        client.locked = true;
        transaction.last_operation = OperationType::Chargeback;

        Ok(())
    }

    pub fn send_logs(&mut self, w: Sender<(ClientID, Client)>) -> AnyResult<()> {
        for (id, client) in self.clients.iter() {
            w.send((*id, *client))?;
        }
        Ok(())
    }

    fn validate_amount(amount: Decimal) -> Result<(), PaymentsError> {
        if amount <= dec!(0) {
            return Err(PaymentsError::AmountNegative(amount));
        }
        if amount.fract() % dec!(0.0001) > dec!(0.) {
            return Err(PaymentsError::AmountPrecisionToHigh(amount));
        }
        Ok(())
    }

    pub fn obtain_transaction(
        transactions: &mut HashMapFnv<TransactionID, Transaction>,
        transaction_id: TransactionID,
        client_id: ClientID,
    ) -> Result<&mut Transaction, PaymentsError> {
        let transaction = match transactions.get_mut(&transaction_id) {
            None => return Err(PaymentsError::TransactionNotExists(transaction_id)),
            Some(t) => t,
        };
        if transaction.client_id != client_id {
            return Err(PaymentsError::TransactionNotExists(transaction_id));
        };
        Ok(transaction)
    }

    fn obtain_client(
        clients: &mut HashMapFnv<ClientID, Client>,
        client_id: ClientID,
    ) -> Result<&mut Client, PaymentsError> {
        let client = match clients.get_mut(&client_id) {
            Some(c) => c,
            None => return Err(PaymentsError::ClientNotExist(client_id)),
        };
        if client.locked {
            return Err(PaymentsError::AccountLocked(client_id));
        }
        Ok(client)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::anyhow;
    use env_logger;
    use log::info;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Off)
            .try_init();
    }

    fn get_client(
        clients: &mut HashMapFnv<ClientID, Client>,
        client_id: ClientID,
    ) -> AnyResult<&mut Client> {
        clients
            .get_mut(&client_id)
            .ok_or(anyhow!("client with id '{}' not found", client_id))
    }

    #[test]
    fn test_deposit() -> AnyResult<()> {
        init();

        struct TestCase {
            input: (u16, u32, Decimal),
            expect: Option<Client>,
            expect_err: bool,
            desc: &'static str,
            is_locked: bool,
        }

        let cases: Vec<TestCase> = vec![
            TestCase {
                desc: "happy path, deposit accepted",
                input: (1234, 10, dec!(10.123)),
                expect: Some(Client {
                    available: dec!(10.123),
                    total: dec!(10.123),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: false,
                is_locked: false,
            },
            TestCase {
                desc: "happy path, deposit increased",
                input: (1234, 11, dec!(15.123)),
                expect: Some(Client {
                    available: dec!(25.246),
                    total: dec!(25.246),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: false,
                is_locked: false,
            },
            TestCase {
                desc: "error, duplicated transaction id",
                input: (1234, 11, dec!(15.123)),
                expect: Some(Client {
                    available: dec!(25.246),
                    total: dec!(25.246),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, negative amount",
                input: (1234, 12, dec!(-15.123)),
                expect: Some(Client {
                    available: dec!(25.246),
                    total: dec!(25.246),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, negative amount",
                input: (1234, 13, dec!(-15.123)),
                expect: Some(Client {
                    available: dec!(25.246),
                    total: dec!(25.246),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, too high precision",
                input: (1234, 14, dec!(15.12345)),
                expect: Some(Client {
                    available: dec!(25.246),
                    total: dec!(25.246),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, account is blocked",
                input: (1234, 15, dec!(10)),
                expect: Some(Client {
                    available: dec!(25.246),
                    total: dec!(25.246),
                    locked: true,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: true,
            },
        ];

        let mut pays = Payments::new();
        for tc in cases {
            info!("starting case: {}", tc.desc);

            if tc.is_locked {
                get_client(&mut pays.clients, tc.input.0).unwrap().locked = true
            }

            assert_eq!(
                pays.deposit(tc.input.0, tc.input.1, tc.input.2).is_err(),
                tc.expect_err
            );

            if let Some(expect) = tc.expect {
                assert_eq!(get_client(&mut pays.clients, tc.input.0).unwrap(), &expect)
            }
        }
        Ok(())
    }

    #[test]
    fn test_withdrawal() -> AnyResult<()> {
        init();

        struct TestCase {
            input: (u16, u32, Decimal),
            expect: Option<Client>,
            expect_err: bool,
            desc: &'static str,
            is_locked: bool,
        }
        let default_client_id: u16 = 1234;
        let mut pays = Payments::new();
        pays.deposit(default_client_id, 1, dec!(10000))?;

        let cases: Vec<TestCase> = vec![
            TestCase {
                desc: "happy path, withdrawal accepted",
                input: (default_client_id, 2, dec!(6000)),
                expect: Some(Client {
                    available: dec!(4000),
                    total: dec!(4000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: false,
                is_locked: false,
            },
            TestCase {
                desc: "happy path, deposit decrease",
                input: (default_client_id, 3, dec!(1000)),
                expect: Some(Client {
                    available: dec!(3000),
                    total: dec!(3000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: false,
                is_locked: false,
            },
            TestCase {
                desc: "error, duplicated transaction id",
                input: (default_client_id, 3, dec!(1000)),
                expect: Some(Client {
                    available: dec!(3000),
                    total: dec!(3000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, negative amount",
                input: (default_client_id, 4, dec!(-1000.1234)),
                expect: Some(Client {
                    available: dec!(3000),
                    total: dec!(3000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, too high precision",
                input: (default_client_id, 5, dec!(1000.12345)),
                expect: Some(Client {
                    available: dec!(3000),
                    total: dec!(3000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, insufficient balance",
                input: (default_client_id, 6, dec!(5000)),
                expect: Some(Client {
                    available: dec!(3000),
                    total: dec!(3000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, account is blocked",
                input: (1234, 15, dec!(10)),
                expect: Some(Client {
                    available: dec!(3000),
                    total: dec!(3000),
                    locked: true,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: true,
            },
        ];

        for tc in cases {
            info!("starting case: {}", tc.desc);

            if tc.is_locked {
                get_client(&mut pays.clients, tc.input.0).unwrap().locked = true
            }

            assert_eq!(
                pays.withdraw(tc.input.0, tc.input.1, tc.input.2).is_err(),
                tc.expect_err
            );

            if let Some(expect) = tc.expect {
                assert_eq!(get_client(&mut pays.clients, tc.input.0).unwrap(), &expect)
            }
        }
        Ok(())
    }

    #[test]
    fn test_dispute() -> AnyResult<()> {
        init();

        struct TestCase {
            input: (u16, u32),
            expect: Option<Client>,
            expect_err: bool,
            desc: &'static str,
            is_locked: bool,
        }
        let default_client_id: u16 = 1234;
        let disputed_transaction_id: u32 = 2;
        let mut pays = Payments::new();
        pays.deposit(default_client_id, 1, dec!(10000))?;
        pays.deposit(default_client_id, disputed_transaction_id, dec!(5000))?;

        let cases: Vec<TestCase> = vec![
            TestCase {
                desc: "happy path, dispute accepted",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(15000),
                    locked: false,
                    held: dec!(5000),
                }),
                expect_err: false,
                is_locked: false,
            },
            TestCase {
                desc: "error, dispute already disputed transaction",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(15000),
                    locked: false,
                    held: dec!(5000),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, non-existing transaction",
                input: (default_client_id, 123453),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(15000),
                    locked: false,
                    held: dec!(5000),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, account is locked",
                input: (default_client_id, 1),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(15000),
                    locked: true,
                    held: dec!(5000),
                }),
                expect_err: true,
                is_locked: true,
            },
            TestCase {
                desc: "error, wrong client id",
                input: (12345, 1),
                expect: None,
                expect_err: true,
                is_locked: false,
            },
        ];

        for tc in cases {
            info!("starting case: {}", tc.desc);

            if tc.is_locked {
                get_client(&mut pays.clients, tc.input.0).unwrap().locked = true
            }

            assert_eq!(pays.dispute(tc.input.0, tc.input.1).is_err(), tc.expect_err);

            if let Some(expect) = tc.expect {
                assert_eq!(get_client(&mut pays.clients, tc.input.0).unwrap(), &expect)
            }
        }
        Ok(())
    }

    #[test]
    fn test_resolve() -> AnyResult<()> {
        init();

        struct TestCase {
            input: (u16, u32),
            expect: Option<Client>,
            expect_err: bool,
            desc: &'static str,
            is_locked: bool,
        }
        let default_client_id: u16 = 1234;
        let normal_transaction_id: u32 = 1;
        let disputed_transaction_id: u32 = 2;
        let mut pays = Payments::new();
        pays.deposit(default_client_id, normal_transaction_id, dec!(10000))?;
        pays.deposit(default_client_id, disputed_transaction_id, dec!(5000))?;
        pays.dispute(default_client_id, disputed_transaction_id)?;

        let cases: Vec<TestCase> = vec![
            TestCase {
                desc: "happy path, resolve accepted",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(15000),
                    total: dec!(15000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: false,
                is_locked: false,
            },
            TestCase {
                desc: "error, resolve already resolved transaction",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(15000),
                    total: dec!(15000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, resolve non-existing transaction",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(15000),
                    total: dec!(15000),
                    locked: false,
                    held: dec!(0),
                }),
                expect_err: true,
                is_locked: false,
            },
            TestCase {
                desc: "error, wrong client id",
                input: (12345, 1),
                expect: None,
                expect_err: true,
                is_locked: false,
            },
        ];

        for tc in cases {
            info!("starting case: {}", tc.desc);

            if tc.is_locked {
                get_client(&mut pays.clients, tc.input.0).unwrap().locked = true
            }

            assert_eq!(pays.resolve(tc.input.0, tc.input.1).is_err(), tc.expect_err);

            if let Some(expect) = tc.expect {
                assert_eq!(get_client(&mut pays.clients, tc.input.0).unwrap(), &expect)
            }
        }
        Ok(())
    }

    #[test]
    fn test_chargeback() -> AnyResult<()> {
        init();

        struct TestCase {
            input: (u16, u32),
            expect: Option<Client>,
            expect_err: bool,
            desc: &'static str,
        }
        let default_client_id: u16 = 1234;
        let normal_transaction_id: u32 = 1;
        let disputed_transaction_id: u32 = 2;
        let mut pays = Payments::new();
        pays.deposit(default_client_id, normal_transaction_id, dec!(10000))?;
        pays.deposit(default_client_id, disputed_transaction_id, dec!(5000))?;
        pays.dispute(default_client_id, disputed_transaction_id)?;

        let cases: Vec<TestCase> = vec![
            TestCase {
                desc: "happy path, chargeback accepted",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(10000),
                    locked: true,
                    held: dec!(0),
                }),
                expect_err: false,
            },
            TestCase {
                desc: "error, chargeback already charge-backed transaction",
                input: (default_client_id, disputed_transaction_id),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(10000),
                    locked: true,
                    held: dec!(0),
                }),
                expect_err: true,
            },
            TestCase {
                desc: "error, chargeback undisputed transaction",
                input: (default_client_id, normal_transaction_id),
                expect: Some(Client {
                    available: dec!(10000),
                    total: dec!(10000),
                    locked: true,
                    held: dec!(0),
                }),
                expect_err: true,
            },
            TestCase {
                desc: "error, chargeback of non-existant transaction",
                input: (default_client_id, 1243532),
                expect: None,
                expect_err: true,
            },
        ];

        for tc in cases {
            info!("starting case: {}", tc.desc);

            assert_eq!(
                pays.chargeback(tc.input.0, tc.input.1).is_err(),
                tc.expect_err
            );

            if let Some(expect) = tc.expect {
                assert_eq!(get_client(&mut pays.clients, tc.input.0).unwrap(), &expect)
            }
        }
        Ok(())
    }
}
