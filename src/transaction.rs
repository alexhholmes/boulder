/// Transactions
/// 
/// Should be lazily executed, like async/await
/// 

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Consistency {
    Optimistic,
    Synchronous,
}

pub struct TransactionHandle {
    
}

impl TransactionHandle {
    pub fn execute(&self, consistency: Consistency) {
        unimplemented!()
    }
    
    pub fn default(&self) {
        self.execute(Consistency::Optimistic)
    }
}

pub struct Transaction {
    
}

impl Transaction {
}