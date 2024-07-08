use rand::{distributions::Alphanumeric, Rng};

pub mod serde_hex_bytes;
pub mod serde_utf8_bytes;

pub fn generate_random_ascii_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
