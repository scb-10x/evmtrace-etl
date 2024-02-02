use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::{aliases::B32, Address};
use anyhow::{anyhow, bail, Result};
use futures_util::StreamExt;
use log::info;
use once_cell::sync::Lazy;
use rdkafka::{
    config::FromClientConfig,
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    util::DefaultRuntime,
    Message, Offset, TopicPartitionList,
};
use serde_json::from_str;
use tokio::{
    spawn,
    sync::broadcast::{channel, Sender},
    task::{JoinHandle, JoinSet},
};

use crate::{
    config::CONFIG,
    traces::{Contract, GasUsed, Trace, TraceResult, Transaction},
};

use super::{EC_MUL_ADDRESS, EC_PAIRING_ADDRESS};

pub static TRACE_CONSUMER: Lazy<TraceConsumer> = Lazy::new(|| TraceConsumer::new());

pub type TraceStreamConsumer = Arc<StreamConsumer<DefaultConsumerContext, DefaultRuntime>>;
pub struct TraceConsumer {
    pub consumers: HashMap<&'static str, (u64, TraceStreamConsumer)>,
    pub tx: Sender<(Vec<TraceResult>, (&'static str, TopicPartitionList))>,
}

impl TraceConsumer {
    pub fn new() -> Self {
        let config = CONFIG.kafka_config();
        let consumers = CONFIG
            .chains
            .iter()
            .map(|c| {
                let consumer =
                    StreamConsumer::<DefaultConsumerContext, DefaultRuntime>::from_config(&config)
                        .expect("Failed to create consumer");
                consumer
                    .subscribe(&[&c.kafka_trace_topic])
                    .expect("Failed to subscribe to topic");
                (c.kafka_trace_topic.as_str(), (c.id, Arc::new(consumer)))
            })
            .collect::<HashMap<_, _>>();
        let (tx, _) = channel(100_000);
        Self { tx, consumers }
    }

    pub fn commit(&self, topic_id: &str, topic_partition_list: TopicPartitionList) -> Result<()> {
        if let Some((_, consumer)) = self.consumers.get(topic_id) {
            consumer.commit(&topic_partition_list, CommitMode::Async)?;
        }
        Ok(())
    }

    pub fn poll(&self) -> JoinHandle<Result<()>> {
        let mut set = JoinSet::<Result<()>>::new();
        for (topic_id, (chain_id, consumer)) in &self.consumers {
            let topic_id = *topic_id;
            let chain_id = *chain_id;
            let consumer = consumer.clone();
            let tx = self.tx.clone();
            set.spawn(async move {
                // to_address -> from_address -> count
                let mut call_tree = HashMap::<Address, HashMap<Address, u16>>::new();
                // from_address -> to_address -> gas_used
                let mut gas_tree = HashMap::<Address, HashMap<Address, u64>>::new();
                // to_address -> function_signature
                let mut signature_tree = HashMap::<Address, HashSet<B32>>::new();
                // from_address -> input_size
                let mut ec_pairing_input_size_tree = HashMap::<Address, Vec<u32>>::new();
                let mut first_trace: Option<Trace> = None;

                let mut stream = consumer.stream();
                info!("Starting trace consumer for {}", topic_id);
                while let Some(msg) = stream.next().await {
                    let m = msg?;
                    let payload = m
                        .payload_view::<str>()
                        .and_then(|p| p.ok())
                        .ok_or_else(|| anyhow!("Invalid payload"))?;
                    let trace = from_str::<Trace>(payload)
                        .map_err(|e| anyhow!("Serialization Error: {e}, original {payload}"))?;

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
                        true,
                    ) = (
                        &first_trace,
                        trace.trace_address.is_empty(),
                        call_tree.contains_key(&EC_PAIRING_ADDRESS),
                    ) {
                        let first_degree_callers: HashSet<Address> = call_tree
                            .get(&EC_PAIRING_ADDRESS)
                            .map(|e| e.keys().copied().collect())
                            .unwrap_or_default();

                        let second_degree_callers: HashSet<Address> = first_degree_callers
                            .iter()
                            .filter_map(|a| call_tree.get(a))
                            .flat_map(|e| e.keys())
                            .copied()
                            .collect();

                        let contracts: Vec<TraceResult> = first_degree_callers
                            .iter()
                            .map(|e| (e, 0, HashSet::from([EC_PAIRING_ADDRESS])))
                            .chain(second_degree_callers.iter().map(|e| (e, 1, HashSet::new())))
                            .map(|(a, degree, _)| {
                                Contract {
                                    chain_id,
                                    address: *a,
                                    function_signatures: signature_tree
                                        .get(a)
                                        .cloned()
                                        .unwrap_or_default(),
                                    degree,
                                    ec_mul_count: call_tree
                                        .get(&EC_MUL_ADDRESS)
                                        .and_then(|m| m.get(a))
                                        .copied()
                                        .unwrap_or_default(),
                                    ec_pairing_count: call_tree
                                        .get(&EC_PAIRING_ADDRESS)
                                        .and_then(|m| m.get(a))
                                        .copied()
                                        .unwrap_or_default(),
                                    ec_pairing_input_sizes: ec_pairing_input_size_tree
                                        .get(a)
                                        .cloned()
                                        .unwrap_or_default(),
                                    call: call_tree
                                        .get(&EC_PAIRING_ADDRESS)
                                        .map(|m| m.keys().collect::<HashSet<_>>())
                                        .unwrap_or_default()
                                        .intersection(
                                            &gas_tree
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
                            .filter_map(|a| gas_tree.get(a))
                            .flat_map(|e| e.values())
                            .sum();

                        let second_degree_gas_used: u64 = second_degree_callers
                            .iter()
                            .filter_map(|a| gas_tree.get(a))
                            .flat_map(|e| e.values())
                            .sum();

                        let transaction: TraceResult = Transaction {
                            chain_id,
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

                        let tx = tx.clone();

                        let mut topic_partition = TopicPartitionList::new();
                        topic_partition.add_partition_offset(
                            m.topic(),
                            m.partition(),
                            Offset::from_raw(m.offset() - 1),
                        )?;
                        spawn(async move {
                            tx.send((
                                contracts.into_iter().chain([transaction]).collect(),
                                (topic_id, topic_partition),
                            ))
                            .ok();
                        });
                    }

                    if trace.trace_address.is_empty() {
                        call_tree.clear();
                        gas_tree.clear();
                        ec_pairing_input_size_tree.clear();
                        first_trace = Some(trace.clone());
                    }

                    if let (Some(from_address), Some(to_address)) =
                        (trace.from_address, trace.to_address)
                    {
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

                        signature_tree
                            .entry(to_address)
                            .or_default()
                            .insert(function_signature);
                        call_tree
                            .entry(to_address)
                            .or_default()
                            .entry(from_address)
                            .and_modify(|c| *c += 1)
                            .or_insert(1);
                        gas_tree
                            .entry(from_address)
                            .or_default()
                            .entry(to_address)
                            .and_modify(|g| *g += trace.gas_used.unwrap_or_default())
                            .or_insert(trace.gas_used.unwrap_or_default());

                        if to_address == EC_PAIRING_ADDRESS {
                            ec_pairing_input_size_tree
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
                Ok(())
            });
        }

        spawn(async move {
            while let Some(r) = set.join_next().await {
                match r {
                    Err(e) => bail!(e),
                    Ok(Err(e)) => bail!(e),
                    _ => {}
                };
            }
            Ok(())
        })
    }
}
