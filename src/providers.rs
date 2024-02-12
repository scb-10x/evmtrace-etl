use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use ethers::providers::{Http, Provider, Ws};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::config::{Chain, CONFIG};

pub static PROVIDER_POOL: Lazy<ProviderPool> = Lazy::new(ProviderPool::new);

#[derive(Debug, Default)]
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
        let rpc_url = CONFIG
            .chains
            .iter()
            .find_map(|c| match c {
                Chain::Provider(chain) => (chain.id == chain_id).then_some(chain.rpc_url.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("No RPC provider for chain {}", chain_id))?;
        let provider = Arc::new(Provider::<Http>::try_from(rpc_url)?);
        rpc.insert(chain_id, provider.clone());
        Ok(provider)
    }

    pub async fn get_ws(&self, chain_id: u64) -> Result<Arc<Provider<Ws>>> {
        let mut ws = self.ws.write().await;
        if let Some(provider) = ws.get(&chain_id) {
            return Ok(provider.clone());
        }
        let ws_url = CONFIG
            .chains
            .iter()
            .find_map(|c| match c {
                Chain::Provider(chain) => (chain.id == chain_id).then_some(chain.ws_url.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("No WS provider for chain {}", chain_id))?;
        let provider = Arc::new(Provider::<Ws>::connect(ws_url).await?);
        ws.insert(chain_id, provider.clone());
        Ok(provider)
    }
}
