use anyhow::{bail, Result};
use futures_util::future::pending;
use tokio::{
    spawn,
    task::{JoinHandle, JoinSet},
};

/// Join a set of futures, if the set is empty, return a pending future
pub fn join_set_else_pending(mut set: JoinSet<Result<()>>) -> JoinHandle<Result<()>> {
    spawn(async move {
        match set.is_empty() {
            true => pending::<()>().await,
            false => {
                while let Some(r) = set.join_next().await {
                    match r {
                        Err(e) => bail!(e),
                        Ok(Err(e)) => bail!(e),
                        _ => {}
                    };
                }
            }
        };
        Ok(())
    })
}
