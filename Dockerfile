ARG APP_NAME=zkscan-etl
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
RUN apt update && apt install -y cmake capnproto libsasl2-dev
WORKDIR /app

ARG APP_NAME
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

ARG APP_NAME
FROM chef AS builder 
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin $APP_NAME

ARG APP_NAME
# We do not need the Rust toolchain to run the binary!
FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/$APP_NAME /usr/local/bin
ENTRYPOINT ["/usr/local/bin/$APP_NAME"]
