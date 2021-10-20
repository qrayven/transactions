use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

pub type ClientID = u16;
pub type TransactionID = u32;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OperationType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl OperationType {
    pub fn is_disputable(&self) -> bool {
        matches!(self, &OperationType::Deposit | &OperationType::Resolve)
    }
}

#[derive(Debug)]
pub struct Transaction {
    pub client_id: ClientID,
    pub last_operation: OperationType,
    pub amount: Decimal,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct Client {
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

impl std::fmt::Display for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.available.round_dp(4),
            self.held.round_dp(4),
            self.total.round_dp(4),
            self.locked
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationRecord {
    #[serde(rename = "type")]
    pub operation: OperationType,
    #[serde(rename = "client")]
    pub client_id: ClientID,
    #[serde(rename = "tx")]
    pub transaction_id: TransactionID,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Decimal>,
}
