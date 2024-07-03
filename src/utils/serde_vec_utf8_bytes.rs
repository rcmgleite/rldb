use std::fmt;

use bytes::Bytes;
use serde::de::{SeqAccess, Visitor};
use serde::Serialize;
use serde::{Deserializer, Serializer};

pub fn serialize<S: Serializer>(v: &Vec<Bytes>, s: S) -> Result<S::Ok, S::Error> {
    let col = v.iter().map(|b| String::from_utf8_lossy(b)).collect();
    Vec::serialize(&col, s)
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Bytes>, D::Error> {
    struct VecParser;
    impl<'de> Visitor<'de> for VecParser {
        type Value = Vec<Bytes>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("[bytes]")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut v = Vec::new();

            while let Some(elem) = seq.next_element::<String>()? {
                v.push(Bytes::from(elem));
            }

            Ok(v)
        }
    }

    d.deserialize_any(VecParser {})
}
