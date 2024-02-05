use axum::{http::StatusCode, routing::get, Json, Router};
use serde_json::{json, Map, Value};

mod stats;
pub use stats::STATS;

use self::stats::Stats;

pub fn routes() -> Router {
    Router::new().route("/health", get(health))
}

pub async fn health() -> (StatusCode, Json<Value>) {
    let mut output = Map::<String, Value>::new();
    output.insert("queue".to_string(), json!(Stats::queue()));
    for (k, v) in STATS.read().await {
        output.insert(k, v);
    }

    (
        StatusCode::OK,
        Json(json!({
            "health": "OK",
            "stats": output,
        })),
    )
}
