use crate::{
    error::{Error, InvalidRequest, Result},
    persistency::versioning::version_vector::{ProcessId, VersionVector},
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
            version: VersionVector::deserialize(pid, hex_decoded)?,
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
    version: VersionVector,
}

impl Context {
    pub fn merge_version(&mut self, version: &VersionVector) {
        self.version.merge(version);
    }

    pub fn serialize(self) -> SerializedContext {
        SerializedContext(hex::encode(self.version.serialize()))
    }
}

impl Into<VersionVector> for Context {
    fn into(self) -> VersionVector {
        self.version
    }
}

impl From<VersionVector> for Context {
    fn from(version: VersionVector) -> Self {
        Self { version }
    }
}
