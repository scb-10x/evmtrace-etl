use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use ethers::providers::{Http, Provider, Ws};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::config::CONFIG;

pub static PROVIDER_POOL: Lazy<ProviderPool> = Lazy::new(ProviderPool::new);

pub struct ProviderPool {
    rpc: RwLock<HashMap<u64, Arc<Provider<Http>>>>,
    ws: RwLock<HashMap<u64, Arc<Provider<Ws>>>>,
}

impl ProviderPool {
    pub fn new() -> Self {
        Self {
            rpc: RwLock::new(HashMap::new()),
            ws: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_rpc(&self, chain_id: u64) -> Result<Arc<Provider<Http>>> {
        let mut rpc = self.rpc.write().await;
        if let Some(provider) = rpc.get(&chain_id) {
            return Ok(provider.clone());
        }
        let chain = CONFIG
            .chains
            .iter()
            .find(|c| c.id == chain_id)
            .ok_or_else(|| anyhow!("No RPC provider for chain {}", chain_id))?;
        let provider = Arc::new(Provider::<Http>::try_from(&chain.rpc_url)?);
        rpc.insert(chain_id, provider.clone());
        Ok(provider)
    }

    pub async fn get_ws(&self, chain_id: u64) -> Result<Arc<Provider<Ws>>> {
        let mut ws = self.ws.write().await;
        if let Some(provider) = ws.get(&chain_id) {
            return Ok(provider.clone());
        }
        let chain = CONFIG
            .chains
            .iter()
            .find(|c| c.id == chain_id)
            .ok_or_else(|| anyhow!("No WS provider for chain {}", chain_id))?;
        let provider = Arc::new(Provider::<Ws>::connect(&chain.ws_url).await?);
        ws.insert(chain_id, provider.clone());
        Ok(provider)
    }
}
