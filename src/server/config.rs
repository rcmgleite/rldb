use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Config {
    #[serde(flatten)]
    pub cluster_type: ClusterType,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterType {
    Standalone(StandaloneConfig),
    Cluster(ClusterConfig),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StandaloneConfig {
    pub port: u16,
    pub storage_engine: StorageEngine,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageEngine {
    InMemory,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClusterConfig {
    pub port: u16,
    pub storage_engine: StorageEngine,
    pub partitioning_scheme: PartitioningScheme,
    pub gossip: Gossip,
    pub quorum: Quorum,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Gossip {
    pub port: u16,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Quorum {
    #[serde(rename = "n")]
    pub replicas: usize,
    #[serde(rename = "r")]
    pub reads: usize,
    #[serde(rename = "w")]
    pub writes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningScheme {
    ConsistentHashing,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::server::config::{ClusterConfig, Gossip, PartitioningScheme, Quorum};

    use super::{ClusterType, Config, StandaloneConfig, StorageEngine};

    #[test]
    fn deserialize_standalone() {
        let mut standalone_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        standalone_config_path.push("conf/standalone.json");

        let standalone_stringified_json = std::fs::read_to_string(standalone_config_path).unwrap();

        let config: Config = serde_json::from_str(&standalone_stringified_json).unwrap();

        assert!(matches!(
            config,
            Config {
                cluster_type: ClusterType::Standalone(StandaloneConfig {
                    port: 3001,
                    storage_engine: StorageEngine::InMemory,
                }),
            }
        ));
    }

    #[test]
    fn deserialize_cluster() {
        let mut standalone_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        standalone_config_path.push("conf/cluster_node_1.json");

        let standalone_stringified_json = std::fs::read_to_string(standalone_config_path).unwrap();

        let config: Config = serde_json::from_str(&standalone_stringified_json).unwrap();

        assert!(matches!(
            config,
            Config {
                cluster_type: ClusterType::Cluster(ClusterConfig {
                    port: 3001,
                    storage_engine: StorageEngine::InMemory,
                    partitioning_scheme: PartitioningScheme::ConsistentHashing,
                    quorum: Quorum {
                        replicas: 3,
                        reads: 1,
                        writes: 2,
                    },
                    gossip: Gossip { port: 4001 }
                }),
            }
        ));
    }
}
