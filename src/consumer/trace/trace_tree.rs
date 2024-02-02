use std::collections::{HashMap, HashSet};

use alloy_primitives::{aliases::B32, Address};

use crate::{
    consumer::{EC_MUL_ADDRESS, EC_PAIRING_ADDRESS},
    traces::{Contract, GasUsed, Trace, TraceResult, Transaction},
};

pub struct TraceTree {
    pub topic_id: &'static str,
    pub chain_id: u64,
    /// to_address -> from_address -> count
    pub call_tree: HashMap<Address, HashMap<Address, u16>>,
    /// from_address -> to_address -> gas_used
    pub gas_tree: HashMap<Address, HashMap<Address, u64>>,
    /// to_address -> function_signature
    pub signature_tree: HashMap<Address, HashSet<B32>>,
    /// from_address -> input_size
    pub ec_pairing_input_size_tree: HashMap<Address, Vec<u32>>,
    pub first_trace: Option<Trace>,
}

impl TraceTree {
    pub fn new(topic_id: &'static str, chain_id: u64) -> Self {
        Self {
            topic_id,
            chain_id,
            call_tree: HashMap::new(),
            gas_tree: HashMap::new(),
            signature_tree: HashMap::new(),
            ec_pairing_input_size_tree: HashMap::new(),
            first_trace: None,
        }
    }

    pub fn commit(&self) -> Option<Vec<TraceResult>> {
        if let (
            Some(Trace {
                transaction_hash: Some(tx_hash),
                transaction_index: Some(tx_index),
                from_address: Some(from_address),
                to_address: Some(to_address),
                block_number,
                block_timestamp,
                block_hash,
                input,
                value,
                gas,
                gas_used,
                ..
            }),
            true,
        ) = (
            &self.first_trace,
            self.call_tree.contains_key(&EC_PAIRING_ADDRESS),
        ) {
            let first_degree_callers: HashSet<Address> = self
                .call_tree
                .get(&EC_PAIRING_ADDRESS)
                .map(|e| e.keys().copied().collect())
                .unwrap_or_default();

            let second_degree_callers: HashSet<Address> = first_degree_callers
                .iter()
                .filter_map(|a| self.call_tree.get(a))
                .flat_map(|e| e.keys())
                .copied()
                .collect();

            let contracts: Vec<TraceResult> = first_degree_callers
                .iter()
                .map(|e| (e, 0, HashSet::from([EC_PAIRING_ADDRESS])))
                .chain(second_degree_callers.iter().map(|e| (e, 1, HashSet::new())))
                .map(|(a, degree, _)| {
                    Contract {
                        chain_id: self.chain_id,
                        address: *a,
                        function_signatures: self
                            .signature_tree
                            .get(a)
                            .cloned()
                            .unwrap_or_default(),
                        degree,
                        ec_mul_count: self
                            .call_tree
                            .get(&EC_MUL_ADDRESS)
                            .and_then(|m| m.get(a))
                            .copied()
                            .unwrap_or_default(),
                        ec_pairing_count: self
                            .call_tree
                            .get(&EC_PAIRING_ADDRESS)
                            .and_then(|m| m.get(a))
                            .copied()
                            .unwrap_or_default(),
                        ec_pairing_input_sizes: self
                            .ec_pairing_input_size_tree
                            .get(a)
                            .cloned()
                            .unwrap_or_default(),
                        call: self
                            .call_tree
                            .get(&EC_PAIRING_ADDRESS)
                            .map(|m| m.keys().collect::<HashSet<_>>())
                            .unwrap_or_default()
                            .intersection(
                                &self
                                    .gas_tree
                                    .get(a)
                                    .map(|m| m.keys().collect::<HashSet<_>>())
                                    .unwrap_or_default(),
                            )
                            .copied()
                            .copied()
                            .collect(),
                    }
                    .into()
                })
                .collect();

            let first_degree_gas_used: u64 = first_degree_callers
                .iter()
                .filter_map(|a| self.gas_tree.get(a))
                .flat_map(|e| e.values())
                .sum();

            let second_degree_gas_used: u64 = second_degree_callers
                .iter()
                .filter_map(|a| self.gas_tree.get(a))
                .flat_map(|e| e.values())
                .sum();

            let transaction: TraceResult = Transaction {
                chain_id: self.chain_id,
                from_address: *from_address,
                to_address: *to_address,
                closest_address: match second_degree_callers.len() {
                    0 => first_degree_callers,
                    _ => second_degree_callers,
                },
                function_signature: {
                    input
                        .as_ref()
                        .map(|i| {
                            let mut signature = [0u8; 4];
                            match i.len() > 4 {
                                false => B32::new(signature),
                                true => {
                                    signature.copy_from_slice(&i[..4]);
                                    B32::new(signature)
                                }
                            }
                        })
                        .unwrap_or_default()
                },
                transaction_hash: *tx_hash,
                transaction_index: *tx_index,
                block_number: *block_number,
                block_timestamp: *block_timestamp,
                block_hash: *block_hash,
                value: value.unwrap_or_default(),
                input: input.as_ref().cloned().unwrap_or_default(),
                gas_used: GasUsed {
                    requested: gas.unwrap_or_default(),
                    total: gas_used.unwrap_or_default(),
                    first_degree: first_degree_gas_used,
                    second_degree: second_degree_gas_used,
                },
            }
            .into();

            Some(contracts.into_iter().chain([transaction]).collect())
        } else {
            None
        }
    }

    pub fn add_trace<T: AsRef<Trace>>(&mut self, trace: T) {
        let trace = trace.as_ref();
        if let (Some(from_address), Some(to_address)) = (trace.from_address, trace.to_address) {
            let function_signature = trace
                .input
                .as_ref()
                .map(|i| {
                    let mut signature = [0u8; 4];
                    match (to_address, i.len() > 4) {
                        (EC_PAIRING_ADDRESS, _) | (_, false) => B32::new(signature),
                        (_, true) => {
                            signature.copy_from_slice(&i[..4]);
                            B32::new(signature)
                        }
                    }
                })
                .unwrap_or_default();

            self.signature_tree
                .entry(to_address)
                .or_default()
                .insert(function_signature);
            self.call_tree
                .entry(to_address)
                .or_default()
                .entry(from_address)
                .and_modify(|c| *c += 1)
                .or_insert(1);
            self.gas_tree
                .entry(from_address)
                .or_default()
                .entry(to_address)
                .and_modify(|g| *g += trace.gas_used.unwrap_or_default())
                .or_insert(trace.gas_used.unwrap_or_default());

            if to_address == EC_PAIRING_ADDRESS {
                self.ec_pairing_input_size_tree
                    .entry(from_address)
                    .or_default()
                    .push(
                        trace
                            .input
                            .as_ref()
                            .map(|i| i.len() as u32)
                            .unwrap_or_default(),
                    );
            }
        }
    }

    pub fn reset<T: AsRef<Trace>>(&mut self, first_trace: T) {
        self.call_tree.clear();
        self.gas_tree.clear();
        self.signature_tree.clear();
        self.ec_pairing_input_size_tree.clear();
        self.first_trace = Some(first_trace.as_ref().clone());
    }
}
