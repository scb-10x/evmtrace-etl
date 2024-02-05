# ZKSCAN ETL

Pure Rust kafka consumer for trace call filtering with kv caching through redis and dump to postgres ez

## Architecture

- For Ethereum, Arbitrum, and Optimism
- Pull [Chainbase Kafka](https://console.chainbase.com/sync/kafka)'s trace and block topic
- Block
  - Push to etl result channel
- Trace
  - Construct trace tree for each transaction
  - Filter call to precompiles and output as _degree_ from such specific precompiles (0x01 and 0x08)
  - Push all related degree 0 and 1 contracts to etl result channel
  - Push transaction with _enough_ relation to those contracts to etl result channel
- Etl result channel receive result from topic transformation
  - Cache unique block/transaction/contract to Redis
  - Dump to PostgreSQL

### Health Check

Probe endpoint can be called to `/` which always return `{"message":"ok"}` and health and sync stats can be check through `/health` which display syncing distance and current height and so on.

## Performance

Eh 1 core and 512mb memory machine is enough (thanks rust), but binary size is kinda big tho so keep that in mind.

## How To Use

### Build and run locally

Install Git and Rust then

```bash
git clone https://github.com/scb-10x/zkscan-etl
cd zkscan-etl/
```

Configure `.env` through `.env.example` example, then build and run through

```bash
cargo build
cargo run --release
```

Or optionally through docker

```bash
docker compose build
docker compose up
```
