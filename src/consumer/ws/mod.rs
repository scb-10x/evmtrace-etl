use anyhow::{bail, Result};
use backon::{ConstantBuilder, Retryable};
use ethers::{providers::Middleware, types::BlockNumber};
use futures_util::StreamExt;
use log::{debug, warn};
use tokio::{
    spawn,
    task::{JoinHandle, JoinSet},
};

use crate::{
    channels::CHANNEL,
    config::CONFIG,
    providers::PROVIDER_POOL,
    types::{Block, BlockWithChainId, Trace, TraceTree},
};

#[derive(Debug, Clone)]
pub struct WebSocketConsumer;

impl WebSocketConsumer {
    pub fn poll() -> JoinHandle<Result<()>> {
        let mut set = JoinSet::<Result<()>>::new();
        for chain in &CONFIG.chains {
            set.spawn(async move {
                let ws = PROVIDER_POOL.get_ws(chain.id).await?;
                let rpc = PROVIDER_POOL.get_rpc(chain.id).await?;

                let mut stream = ws.subscribe_blocks().await?;
                let mut trace_tree = TraceTree::new(chain.id);

                let backoff = ConstantBuilder::default();
                let get_trace = || async { rpc.trace_block(BlockNumber::Latest).await };
                while let Some(b) = stream.next().await {
                    if let Some(mut block) = Block::from_ethers(b) {
                        let traces = get_trace
                            .retry(&backoff)
                            .notify(|err, _| warn!("Error getting trace: {}, retrying", err))
                            .await?
                            .into_iter()
                            .filter_map(Trace::from_ethers)
                            .collect::<Vec<_>>();
                        if let Some(trace) = traces.last() {
                            debug!("Got trace for block {}", trace.block_number);
                            debug!("Trace: {}", trace);
                        };

                        let mut tx_count = 0;
                        for trace in traces {
                            if trace.trace_address.is_empty() {
                                if let Some(results) = trace_tree.commit() {
                                    for result in &results {
                                        debug!("New result: {}", result);
                                    }
                                    CHANNEL.send_result(results, ());
                                }

                                trace_tree.reset(&trace);
                                tx_count += 1;
                            }

                            trace_tree.add_trace(trace);
                        }
                        block.transaction_count = tx_count;

                        debug!("New block: {}", &block);
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
