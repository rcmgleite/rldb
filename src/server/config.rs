//! Server Configuration attributes
//!
//! For examples see the `/conf` directory.
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Config {
    pub port: u16,
    pub quorum: Quorum,
    pub heartbeat: Heartbeat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Quorum {
    #[serde(rename = "n")]
    pub replicas: usize,
    #[serde(rename = "r")]
    pub reads: usize,
    #[serde(rename = "w")]
    pub writes: usize,
}

impl Default for Quorum {
    fn default() -> Self {
        Self {
            replicas: 3,
            reads: 2,
            writes: 2,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Heartbeat {
    pub fanout: usize,
    pub interval: usize,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::server::config::{Heartbeat, Quorum};

    use super::Config;

    #[test]
    fn deserialize_cluster() {
        let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        config_path.push("conf/node_1.json");

        let stringified_json = std::fs::read_to_string(config_path).unwrap();

        let config: Config = serde_json::from_str(&stringified_json).unwrap();

        assert!(matches!(
            config,
            Config {
                port: 3001,
                quorum: Quorum {
                    replicas: 3,
                    reads: 2,
                    writes: 2,
                },
                heartbeat: Heartbeat {
                    fanout: 2,
                    interval: 500,
                }
            }
        ));
    }
}
