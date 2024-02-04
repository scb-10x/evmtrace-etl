use std::{
    collections::HashMap,
    net::Ipv4Addr,
    panic::{set_hook, take_hook},
    process::exit,
    time::Duration,
};

use anyhow::{anyhow, Error, Result};
use axum::{routing::get, serve, Router};
use log::{debug, error, info, warn};
use tokio::{net::TcpListener, select, spawn, time::Instant};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use zkscan_etl::{
    api,
    channels::CHANNEL,
    config::CONFIG,
    consumer::{TopicCommiter, BLOCK_CONSUMER, TRACE_CONSUMER},
    dumper::POSTGRESQL_DUMPER,
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
        .add_directive("tokio_postgres=info".parse()?);
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
                if *v % 1000 == 0 {
                    info!("Received {} result traces from chain {}", v, t.chain_id());
                }
            }
        }
        Result::<()>::Ok(())
    });
    let handle_dump = spawn(async move {
        let mut rx = CHANNEL.result_tx.subscribe();

        let mut buffer = vec![];
        let mut latest_partition: Option<TopicCommiter> = None;
        let mut last_commit = Instant::now();
        while let Ok((t, partition)) = rx.recv().await {
            buffer.extend(t);

            match (!rx.is_empty(), buffer.len() > 20000) {
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
        e = TRACE_CONSUMER.poll() => e,
        e = BLOCK_CONSUMER.poll() => e,
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
