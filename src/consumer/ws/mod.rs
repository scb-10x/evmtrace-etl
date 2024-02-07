use anyhow::Result;
use backon::{ConstantBuilder, Retryable};
use ethers::{providers::Middleware, types::BlockNumber};
use futures_util::StreamExt;
use log::{debug, info, warn};
use tokio::task::{JoinHandle, JoinSet};

use crate::{
    channels::CHANNEL,
    config::{Chain, CONFIG},
    providers::PROVIDER_POOL,
    types::{Block, BlockWithChainId, Trace, TraceTree},
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
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            let get_trace = || async {
                                rpc.trace_block(BlockNumber::Number(block.number.into()))
                                    .await
                            };
                            let traces = get_trace
                                .retry(&backoff)
                                .notify(|err, _| warn!("Error getting trace: {}, retrying", err))
                                .await?
                                .into_iter()
                                .filter_map(Trace::from_ethers)
                                .collect::<Vec<_>>();

                            block.transaction_count = traces
                                .last()
                                .and_then(|e| e.transaction_index)
                                .unwrap_or_default();

                            if chain.index_tx {
                                for trace in traces {
                                    if trace.trace_address.is_empty() {
                                        if let Some(results) = trace_tree.commit() {
                                            for result in &results {
                                                debug!("New result: {}", result);
                                            }
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
