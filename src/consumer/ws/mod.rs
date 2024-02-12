use std::time::Duration;

use anyhow::Result;
use backon::{ConstantBuilder, Retryable};
use ethers::{providers::Middleware, types::BlockNumber};
use futures_util::StreamExt;
use log::{error, info};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::sleep,
};

use crate::{
    channels::CHANNEL,
    config::{Chain, CONFIG},
    providers::PROVIDER_POOL,
    types::{Block, BlockWithChainId, GethTraceCall, Trace, TraceTree},
    utils::join_set_else_pending,
};

#[derive(Debug, Clone)]
pub struct WebSocketConsumer;

impl WebSocketConsumer {
    pub fn poll() -> JoinHandle<Result<()>> {
        let mut set = JoinSet::<Result<()>>::new();
        for cc in &CONFIG.chains {
            if let Chain::Provider(chain) = cc {
                set.spawn(async move {
                    if chain.index_block {
                        info!("Starting ws block consumer for {}", chain.id);
                    }
                    if chain.index_tx {
                        info!("Starting ws trace consumer for {}", chain.id);
                    }
                    let ws = PROVIDER_POOL.get_ws(chain.id).await?;
                    let rpc = PROVIDER_POOL.get_rpc(chain.id).await?;

                    let mut stream = ws.subscribe_blocks().await?;
                    let mut trace_tree = TraceTree::new(chain.id);

                    let backoff = ConstantBuilder::default();
                    while let Some(b) = stream.next().await {
                        if let Some(mut block) = Block::from_ethers(b) {
                            let block_number = BlockNumber::Number(block.number.into());
                            let get_transactions = || async {
                                rpc.get_block(block_number)
                                    .await
                                    .map(|block| block.map(|b| b.transactions).unwrap_or_default())
                            };
                            let get_traces = || async {
                                rpc.debug_trace_block_by_number(
                                    Some(block_number),
                                    GethTraceCall::option(),
                                )
                                .await
                            };

                            let transactions = get_transactions
                                .retry(&backoff)
                                .notify(|err, _| {
                                    error!("Error getting transactions from blocks: {:?}", err)
                                })
                                .await?;
                            block.transaction_count = transactions.len() as u32;

                            // if index tx, call debug_trace_block_by_number with non top call
                            if chain.index_tx {
                                // sleep to avoid block not found
                                sleep(Duration::from_millis(500)).await;
                                let traces = get_traces
                                    .retry(&backoff)
                                    .notify(|err, _| error!("Error getting traces: {:?}", err))
                                    .await?;
                                for trace in transactions
                                    .into_iter()
                                    .enumerate()
                                    .zip(traces)
                                    .filter_map(|((i, h), t)| {
                                        GethTraceCall::from_geth_trace(t).map(move |trace| {
                                            trace.0.into_iter().map(move |inner| {
                                                Trace::from_call_frame(
                                                    inner,
                                                    (i + 1) as u32,
                                                    h,
                                                    block.number,
                                                )
                                            })
                                        })
                                    })
                                    .flatten()
                                    .flatten()
                                {
                                    if trace.trace_address.is_empty() {
                                        if let Some(results) = trace_tree.commit() {
                                            CHANNEL.send_result(results, ());
                                        }

                                        trace_tree.reset(&trace);
                                    }

                                    trace_tree.add_trace(trace);
                                }
                            }

                            if chain.index_block {
                                CHANNEL.send_result(
                                    vec![BlockWithChainId {
                                        chain_id: chain.id,
                                        block,
                                    }
                                    .into()],
                                    (),
                                );
                            }
                        }
                    }
                    Ok(())
                });
            }
        }

        join_set_else_pending(set)
    }
}
