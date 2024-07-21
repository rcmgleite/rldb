use std::sync::Arc;

use super::Db;
use crate::client::{db_client::DbClientFactory, Client, Factory as ClientFactory};
use crate::cmd::types::{Context, SerializedContext};
use crate::error::Result;

/// Component responsible for managing the synchronization of nodes when cluster membership changes happen.
///  1. Start synchronization tasks
///  2. Keep track of progress/failures
///  3. Retry and backoff when needed
pub struct SynchronizationManager {}

impl SynchronizationManager {}

/// FIXME: Using the "?" operator here is a mistake -> We can't exit this function on errors
/// otherwise we stop the synchronization process.
async fn synchronization_task(db: Arc<Db>, client_factory: Arc<dyn ClientFactory>) -> Result<()> {
    let keys = db.list_keys()?;
    let own_addr = db.cluster_state.own_addr();
    for key in keys {
        let owner = db.cluster_state.key_owner(&key)?;
        if owner.addr == own_addr {
            // we still own this key.. skip sending it anywhere else
            continue;
        }

        // FIXME: We are creating a new client for every entry. This is dumb
        let mut client = client_factory
            .get(String::from_utf8_lossy(&owner.addr).into())
            .await?;

        let storage_entries = db.local_get(key.clone())?;

        for entry in storage_entries {
            client
                .put(
                    key.clone(),
                    entry.value,
                    Some(Context::from(entry.version).serialize().into()),
                    true,
                )
                .await?;
        }
    }

    Ok(())
}
