use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use dotenvy::from_path_iter;
use tracing::info;

const DOTENV_FILENAME: &str = ".env";
const HTTP_ADDR_ENV: &str = "CARABINER_HTTP_ADDR";
const GRPC_ADDR_ENV: &str = "CARABINER_GRPC_ADDR";

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub http_addr: Option<SocketAddr>,
    pub grpc_addr: Option<SocketAddr>,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        if let Some(path) = hydrate_env_from_files()? {
            info!(env_file = %path.display(), "loaded configuration from env file");
        }
        let database_url = env::var("DATABASE_URL")
            .or_else(|_| env::var("CARABINER_DATABASE_URL"))
            .context("DATABASE_URL missing; set it in the environment or .env file")?;
        let http_addr = parse_socket_addr_from_env(HTTP_ADDR_ENV)?;
        let grpc_addr = parse_socket_addr_from_env(GRPC_ADDR_ENV)?;
        Ok(Self {
            database_url,
            http_addr,
            grpc_addr,
        })
    }
}

fn hydrate_env_from_files() -> Result<Option<PathBuf>> {
    let mut current = env::current_dir().context("failed to resolve current directory")?;
    loop {
        let candidate = current.join(DOTENV_FILENAME);
        if candidate.is_file() {
            load_env_file(&candidate)?;
            return Ok(Some(candidate));
        }
        if !current.pop() {
            break;
        }
    }
    Ok(None)
}

fn load_env_file(path: &Path) -> Result<()> {
    for entry in from_path_iter(path)? {
        let (key, value) = entry?;
        if env::var_os(&key).is_some() {
            continue;
        }
        // SAFETY: set_var is unsafe because callers must avoid concurrent mutations.
        // Configuration loading happens during startup before any worker threads spawn.
        unsafe {
            env::set_var(&key, &value);
        }
    }
    Ok(())
}

fn parse_socket_addr_from_env(key: &str) -> Result<Option<SocketAddr>> {
    let value = match env::var(key) {
        Ok(raw) => raw,
        Err(_) => return Ok(None),
    };
    let addr = value
        .parse::<SocketAddr>()
        .with_context(|| format!("{key} must be a valid socket address, e.g. 0.0.0.0:1234"))?;
    Ok(Some(addr))
}
