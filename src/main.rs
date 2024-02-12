use std::{
    collections::HashMap,
    net::Ipv4Addr,
    panic::{set_hook, take_hook},
    process::exit,
};

use anyhow::{anyhow, Error, Result};
use axum::{routing::get, serve, Router};
use log::{error, info};
use tokio::{net::TcpListener, select, spawn};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use zkscan_etl::{
    api::{self, STATS},
    channels::CHANNEL,
    config::CONFIG,
    consumer::{BlockConsumer, TraceConsumer, WebSocketConsumer},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenvy::dotenv().ok();

    let default_panic = take_hook();
    set_hook(Box::new(move |info| {
        error!("Panic: {}", info);
        default_panic(info);
        exit(1);
    }));

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::DEBUG.into())
        .from_env()?
        .add_directive("tokio_postgres=info".parse()?)
        .add_directive("rustls=info".parse()?)
        .add_directive("ethers_providers=info".parse()?)
        .add_directive("h2=info".parse()?)
        .add_directive("hyper=info".parse()?)
        .add_directive("reqwest=info".parse()?)
        .add_directive("tungstenite=info".parse()?);

    info!("Setting up tracing with filter: {}", filter);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();

    let handle_log = spawn(async move {
        let mut cnt = HashMap::<u64, u64>::new();
        let mut rx = CHANNEL.result_tx.subscribe();

        while let Ok((traces, _)) = rx.recv().await {
            for t in traces {
                let v = cnt.entry(t.chain_id()).and_modify(|e| *e += 1).or_insert(1);
                if *v % 10000 == 0 {
                    info!("Received {} result traces from chain {}", v, t.chain_id());
                }

                #[cfg(feature = "trace-result")]
                {
                    use zkscan_etl::types::EtlResult;
                    match &t {
                        EtlResult::BlockWithChainId(b) => {
                            info!(
                                "Received block {} from chain {}",
                                b.block.number,
                                t.chain_id()
                            );
                        }
                        _ => {
                            info!("Received result from chain {}: {}", t.chain_id(), t);
                        }
                    }
                }
            }
        }
        Result::<()>::Ok(())
    });

    #[cfg(feature = "no-dump")]
    let handle_dump = spawn(async move {
        use futures_util::future::pending;
        pending::<()>().await;
        Result::<()>::Ok(())
    });
    #[cfg(not(feature = "no-dump"))]
    let handle_dump = spawn(async move {
        use log::{debug, warn};
        use std::time::Duration;
        use tokio::time::Instant;
        use zkscan_etl::{
            consumer::{Commiter, TopicCommiter},
            dumper::POSTGRESQL_DUMPER,
        };

        let mut rx = CHANNEL.result_tx.subscribe();

        let mut buffer = vec![];
        let mut latest_partition: Option<TopicCommiter> = None;
        let mut last_commit = Instant::now();
        while let Ok((t, commiter)) = rx.recv().await {
            buffer.extend(t);

            match (!rx.is_empty(), buffer.len() > 100_000) {
                (true, false) => continue,
                _ => {
                    let buffer_len = buffer.len();
                    POSTGRESQL_DUMPER.insert_results(&buffer).await?;
                    buffer.clear();

                    let current_rx_len = rx.len();
                    debug!(
                        "Dumped {} result traces to db, {} to go",
                        buffer_len, current_rx_len
                    );
                    if current_rx_len > 100 {
                        warn!("Too many traces in queue: {}", current_rx_len);
                    }
                }
            }

            if let Commiter::Kafka(partition) = commiter {
                let now = Instant::now();
                match latest_partition {
                    Some(l)
                        if l.topic_id != partition.topic_id
                            && partition.offset > l.offset + 100
                            && last_commit.duration_since(now) > Duration::from_secs(1) =>
                    {
                        l.commit()?;
                        last_commit = now;
                    }
                    _ => {}
                }
                latest_partition = Some(partition);
            }
        }

        Result::<()>::Ok(())
    });

    let server = spawn(async move {
        let app = Router::new()
            .route("/", get(|| async { "Ok" }))
            .nest("/", api::routes());
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, CONFIG.port)).await?;
        info!("Server is listening on http://0.0.0.0:{}", CONFIG.port,);
        serve(listener, app)
            .await
            .map_err(|e| anyhow!("Server error: {}", e))
    });

    match select! {
        e = TraceConsumer::poll() => e,
        e = BlockConsumer::poll() => e,
        e = WebSocketConsumer::poll() => e,
        e = STATS.watch() => e,
        e = handle_log => e,
        e = handle_dump => e,
        e = server => e,
    } {
        Ok(Err(e)) => error!("Error: {}", e),
        Err(e) => error!("Join Error: {}", e),
        _ => {}
    }

    Ok(())
}
