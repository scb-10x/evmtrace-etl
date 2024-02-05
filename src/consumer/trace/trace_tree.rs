use std::{
    collections::{HashMap, HashSet},
    iter,
};

use alloy_primitives::{aliases::B32, Address, Bytes};

use crate::{
    constants::addresses::{
        EC_ADD_ADDRESS, EC_MUL_ADDRESS, EC_PAIRING_ADDRESS, EC_RECOVER_ADDRESS,
    },
    types::{Contract, EtlResult, GasUsed, Trace, Transaction},
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
    const FIRST_DEGREE_FILTER_ADDRESSES: &[Address] = &[EC_PAIRING_ADDRESS, EC_RECOVER_ADDRESS];

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

    pub fn construct_signature(b: &Bytes) -> B32 {
        let mut signature = [0u8; 4];
        match b.len() > 4 {
            false => B32::new(signature),
            true => {
                signature.copy_from_slice(&b[..4]);
                B32::new(signature)
            }
        }
    }

    pub fn construct_signature_with_to(b: (&Bytes, Address)) -> B32 {
        let mut signature = [0u8; 4];
        match (b.1, b.0.len() > 4) {
            (_, false) => B32::new(signature),
            (f, _) if Self::FIRST_DEGREE_FILTER_ADDRESSES.contains(&f) => B32::new(signature),
            (_, true) => {
                signature.copy_from_slice(&b.0[..4]);
                B32::new(signature)
            }
        }
    }

    pub fn commit_filter(&self) -> bool {
        self.call_tree.contains_key(&EC_RECOVER_ADDRESS)
            || self.call_tree.contains_key(&EC_PAIRING_ADDRESS)
    }

    pub fn commit(&self) -> Option<Vec<EtlResult>> {
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
        ) = (&self.first_trace, self.commit_filter())
        {
            // Addresses that called EC_PAIRING_ADDRESS or EC_RECOVER_ADDRESS
            // (address, what it called)
            let mut first_degree_callers = HashMap::<Address, HashSet<Address>>::new();
            Self::FIRST_DEGREE_FILTER_ADDRESSES.iter().for_each(|a| {
                if let Some(m) = self.call_tree.get(a) {
                    m.keys().for_each(|k| {
                        first_degree_callers.entry(*k).or_default().insert(*a);
                    });
                }
            });

            // Addresses that called the first_degree_callers
            let mut second_degree_callers = HashMap::<Address, HashSet<Address>>::new();
            first_degree_callers.iter().for_each(|(a, _)| {
                if let Some(m) = self.call_tree.get(a) {
                    m.keys().for_each(|k| {
                        second_degree_callers.entry(*k).or_default().insert(*a);
                    });
                }
            });
            // Remove eoa from second degree callers
            second_degree_callers.remove(from_address);

            // Construct the contracts
            let contracts: Vec<EtlResult> = first_degree_callers
                .iter()
                .zip(iter::repeat(0))
                .chain(second_degree_callers.iter().zip(iter::repeat(1)))
                .map(|((a, call), degree)| {
                    Contract {
                        chain_id: self.chain_id,
                        address: *a,
                        // all the function signatures that were called
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
                        ec_recover_count: self
                            .call_tree
                            .get(&EC_RECOVER_ADDRESS)
                            .and_then(|m| m.get(a))
                            .copied()
                            .unwrap_or_default(),
                        ec_add_count: self
                            .call_tree
                            .get(&EC_ADD_ADDRESS)
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
                        call: call.clone(),
                    }
                    .into()
                })
                .collect();

            let first_degree_gas_used: u64 = first_degree_callers
                .keys()
                .filter_map(|a| self.gas_tree.get(a))
                .flat_map(|e| e.values())
                .sum();

            let second_degree_gas_used: u64 = second_degree_callers
                .keys()
                .filter_map(|a| self.gas_tree.get(a))
                .flat_map(|e| e.values())
                .sum();

            let transaction: EtlResult = Transaction {
                chain_id: self.chain_id,
                from_address: *from_address,
                to_address: *to_address,
                // The address that called the most, if there are no second degree callers then the first degree callers are the closest
                closest_address: match second_degree_callers.len() {
                    0 => first_degree_callers.keys(),
                    _ => second_degree_callers.keys(),
                }
                .copied()
                .collect(),
                function_signature: {
                    input
                        .as_ref()
                        .map(Self::construct_signature)
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
                .zip(Some(to_address))
                .map(Self::construct_signature_with_to)
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
