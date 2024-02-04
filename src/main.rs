use std::{
    collections::HashMap,
    panic::{set_hook, take_hook},
    process::exit,
};

use anyhow::{Error, Result};
use log::{debug, error, info, warn};
use rdkafka::TopicPartitionList;
use tokio::{select, spawn};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use zkscan_etl::{channels::CHANNEL, consumer::TRACE_CONSUMER, dumper::POSTGRESQL_DUMPER};

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
        let mut latest_partition: Option<(&str, TopicPartitionList)> = None;
        while let Ok((t, partition)) = rx.recv().await {
            buffer.extend(t);

            match (!rx.is_empty(), buffer.len() > 1000) {
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

            match latest_partition {
                Some(l) if l.0 != partition.0 => {
                    TRACE_CONSUMER.commit(l.0, l.1)?;
                }
                _ => {}
            }
            latest_partition = Some(partition);
        }

        Result::<()>::Ok(())
    });

    match select! {
        e = TRACE_CONSUMER.poll() => e,
        e = handle_log => e,
        e = handle_dump => e,
    } {
        Ok(Err(e)) => error!("Error: {}", e),
        Err(e) => error!("Join Error: {}", e),
        _ => {}
    }

    Ok(())
}
