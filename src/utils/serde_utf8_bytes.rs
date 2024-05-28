use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

/// Enable serde to serialize utf8 [`String`] into [`Bytes`]
///
/// # Errors
/// This function returns an error if [`String::from_utf8`] returns an error
pub fn serialize<S: Serializer>(v: &Bytes, s: S) -> Result<S::Ok, S::Error> {
    let stringified = String::from_utf8(v.clone().into()).map_err(|e| {
        serde::ser::Error::custom(format!("Unable to convert bytes into utf8 string - {}", e))
    })?;
    String::serialize(&stringified, s)
}

/// Enable serde to deserialize utf8 [`String`] into [`Bytes`]
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
    let stringified = String::deserialize(d)?;
    Ok(Bytes::from(stringified))
}
