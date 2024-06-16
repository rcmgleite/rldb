use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

/// Enable serde to serialize utf8 [`String`] into [`Bytes`]
///
/// # Errors
/// This function returns an error if [`String::from_utf8`] returns an error
pub fn serialize<S: Serializer>(v: &Option<Bytes>, s: S) -> Result<S::Ok, S::Error> {
    if let Some(v) = v {
        let stringified = String::from_utf8(hex::encode(v).into_bytes()).map_err(|e| {
            serde::ser::Error::custom(format!("Unable to convert bytes into utf8 string - {}", e))
        })?;
        String::serialize(&stringified, s)
    } else {
        String::serialize(&"null".to_string(), s)
    }
}

/// Enable serde to deserialize utf8 [`String`] into [`Bytes`]
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Bytes>, D::Error> {
    let stringified = String::deserialize(d)?;
    if stringified == "null".to_string() {
        return Ok(None);
    }

    let decoded = hex::decode(stringified.into_bytes())
        .map_err(|e| serde::de::Error::custom(format!("Unable to hex::decode {}", e)))?;
    Ok(Some(decoded.into()))
}
