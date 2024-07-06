use std::ops::Deref;

use crate::{
    error::{Error, InvalidRequest, Result},
    persistency::{
        versioning::version_vector::{ProcessId, VersionVector},
        Metadata,
    },
    utils::serde_utf8_bytes,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A [`SerializedContext`] object is a hex encoded bytes array
/// that is returned on Get requests and is used by clients during Put
/// to allow the database to figure out if the incoming Put conflicts with other puts
/// happening to the same key. it basically encodes a [`VersionVector`] that is stored
/// in the database for each value connected to a key. From a client perspective, no knowledge of any of this is needed.
/// The important thing to understand is: Puts HAVE to include this context object as to avoid false conflicts
/// to be flagged by the database.
///
/// # Conflict resolution
/// If a conflict is discovered, both conflicting values will be stored in the database.
/// On a subsequent Get to the given key, both values will be returned alongiside this [`SerializedContext`] object.
/// The client needs to then decide what is the appropriate value for the key and issue another Put - providing
/// both the desired value and the received Context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SerializedContext(String);
impl SerializedContext {
    pub fn new(hex_encoded: String) -> Self {
        Self(hex_encoded)
    }

    pub fn deserialize(self, pid: ProcessId) -> Result<Context> {
        let hex_decoded: Bytes = hex::decode(self.0)
            .map_err(|_| Error::InvalidRequest(InvalidRequest::MalformedContext))?
            .into();

        Ok(Context {
            versions: VersionVector::deserialize(pid, hex_decoded)?,
        })
    }
}

impl From<String> for SerializedContext {
    fn from(v: String) -> Self {
        Self(v)
    }
}

impl Into<String> for SerializedContext {
    fn into(self) -> String {
        self.0
    }
}

/// The deserialized type of [`SerializedContext`].
///
/// For more information, see [`SerializedContext`] docs.
#[derive(Default, Debug)]
pub struct Context {
    versions: VersionVector,
}

impl Context {
    pub fn merge_metadata(&mut self, metadata: &Metadata) {
        self.versions.merge(&metadata.version);
    }

    pub fn serialize(self) -> SerializedContext {
        SerializedContext(hex::encode(self.versions.serialize()))
    }

    pub fn into_metadata(self) -> Metadata {
        Metadata {
            version: self.versions,
        }
    }
}

impl From<Metadata> for Context {
    fn from(value: Metadata) -> Self {
        Self {
            versions: value.version,
        }
    }
}

/// Represents a Value associated with a key in the database.
/// Every value contains:
///  1. Its bytes - the actual bytes a client stored
///  2. a checksum of these bytes
///
/// To make sure no corruptions happened at rest or while fullfilling the request, clients
/// should validate the received bytes against the provided checksum.
///
/// # TODOs
/// This is likely a type that should be inside the [`crate::persistency`] mod and not here...
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Value {
    #[serde(with = "serde_utf8_bytes")]
    value: Bytes,
    crc32c: u32,
}

impl Value {
    pub fn new(value: Bytes, crc32c: u32) -> Self {
        Self { value, crc32c }
    }
}

impl Deref for Value {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
