use axum::{http::StatusCode, routing::get, Json, Router};
use serde_json::{json, Value};

use crate::channels::CHANNEL;

pub fn routes() -> Router {
    Router::new().route("/health", get(health))
}

pub async fn health() -> (StatusCode, Json<Value>) {
    let trace_queue = CHANNEL.result_tx.len();
    (
        StatusCode::OK,
        Json(json!({
            "message": "OK",
            "trace_dump_queue": trace_queue,
        })),
    )
}
