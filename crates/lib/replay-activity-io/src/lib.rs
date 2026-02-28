use std::path::Path;

use anyhow::Result;
use tokio::fs;
use waymark_replay_activity_core::BackendActivity;

pub async fn load_activity_log(path: &Path) -> Result<Vec<BackendActivity>> {
    let bytes = fs::read(path).await?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

pub async fn persist_activity_log(path: &Path, activity: &[BackendActivity]) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).await?;
    }
    let bytes = serde_json::to_vec_pretty(activity)?;
    fs::write(path, bytes).await?;
    Ok(())
}
