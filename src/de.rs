use std::sync::Arc;

use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::forward_to_deserialize_any;

use crate::error::Error;
use crate::proto::google::spanner::v1 as pb;
use crate::result::ColumnMeta;

/// Deserializes a Row by consuming it — moves owned strings instead of cloning.
pub(crate) struct IntoRowDeserializer {
    columns: Arc<Vec<ColumnMeta>>,
    values: Vec<prost_types::Value>,
}

impl IntoRowDeserializer {
    pub(crate) fn new(columns: Arc<Vec<ColumnMeta>>, values: Vec<prost_types::Value>) -> Self {
        Self { columns, values }
    }
}

impl<'de> de::Deserializer<'de> for IntoRowDeserializer {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_map(IntoRowMapAccess {
            columns: self.columns,
            values: self.values,
            index: 0,
        })
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}

struct IntoRowMapAccess {
    columns: Arc<Vec<ColumnMeta>>,
    values: Vec<prost_types::Value>,
    index: usize,
}

impl<'de> MapAccess<'de> for IntoRowMapAccess {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        if self.index >= self.columns.len() {
            return Ok(None);
        }
        let key = self.columns[self.index].name.as_str();
        seed.deserialize(key.into_deserializer()).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let idx = self.index;
        let col = self.columns.get(idx).ok_or_else(|| {
            Error::Deserialize("row metadata/value cursor out of sync".to_string())
        })?;
        if idx >= self.values.len() {
            return Err(Error::Deserialize(format!(
                "row missing value for column '{}'",
                col.name
            )));
        }
        let val = std::mem::take(&mut self.values[idx]);
        self.index += 1;
        seed.deserialize(IntoValueDeserializer {
            value: val,
            spanner_type: &col.spanner_type,
        })
    }
}

/// Deserializes a single owned protobuf Value — moves strings instead of cloning.
struct IntoValueDeserializer<'a> {
    value: prost_types::Value,
    spanner_type: &'a pb::Type,
}

impl<'de, 'a> de::Deserializer<'de> for IntoValueDeserializer<'a> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        use prost_types::value::Kind;

        let kind = match self.value.kind {
            None | Some(Kind::NullValue(_)) => return visitor.visit_none(),
            Some(k) => k,
        };

        let code =
            pb::TypeCode::try_from(self.spanner_type.code).unwrap_or(pb::TypeCode::Unspecified);

        match code {
            pb::TypeCode::Bool => match kind {
                Kind::BoolValue(b) => visitor.visit_bool(b),
                _ => Err(Error::Deserialize(
                    "expected bool value for BOOL column".to_string(),
                )),
            },
            pb::TypeCode::Int64 => match kind {
                Kind::StringValue(s) => {
                    let n: i64 = s.parse().map_err(|e| {
                        Error::Deserialize(format!("invalid INT64 value '{s}': {e}"))
                    })?;
                    visitor.visit_i64(n)
                }
                _ => Err(Error::Deserialize(
                    "expected string value for INT64 column".into(),
                )),
            },
            pb::TypeCode::Float64 => match kind {
                Kind::NumberValue(n) => visitor.visit_f64(n),
                Kind::StringValue(s) => {
                    let n: f64 = match s.as_str() {
                        "NaN" => f64::NAN,
                        "Infinity" => f64::INFINITY,
                        "-Infinity" => f64::NEG_INFINITY,
                        _ => s.parse().map_err(|e| {
                            Error::Deserialize(format!("invalid FLOAT64 value '{s}': {e}"))
                        })?,
                    };
                    visitor.visit_f64(n)
                }
                _ => Err(Error::Deserialize(
                    "expected number value for FLOAT64 column".into(),
                )),
            },
            pb::TypeCode::Float32 => match kind {
                Kind::NumberValue(n) => visitor.visit_f32(n as f32),
                Kind::StringValue(s) => {
                    let n: f32 = match s.as_str() {
                        "NaN" => f32::NAN,
                        "Infinity" => f32::INFINITY,
                        "-Infinity" => f32::NEG_INFINITY,
                        _ => s.parse().map_err(|e| {
                            Error::Deserialize(format!("invalid FLOAT32 value '{s}': {e}"))
                        })?,
                    };
                    visitor.visit_f32(n)
                }
                _ => Err(Error::Deserialize(
                    "expected number value for FLOAT32 column".into(),
                )),
            },
            pb::TypeCode::String
            | pb::TypeCode::Timestamp
            | pb::TypeCode::Date
            | pb::TypeCode::Numeric
            | pb::TypeCode::Json => match kind {
                Kind::StringValue(s) => visitor.visit_string(s),
                _ => Err(Error::Deserialize(format!(
                    "expected string value for {code:?} column"
                ))),
            },
            pb::TypeCode::Bytes => match kind {
                Kind::StringValue(s) => {
                    use base64::Engine;
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(&s)
                        .map_err(|e| {
                            Error::Deserialize(format!("invalid base64 BYTES value: {e}"))
                        })?;
                    visitor.visit_byte_buf(bytes)
                }
                _ => Err(Error::Deserialize(
                    "expected string value for BYTES column".into(),
                )),
            },
            pb::TypeCode::Array => match kind {
                Kind::ListValue(list) => {
                    let elem_type =
                        self.spanner_type
                            .array_element_type
                            .as_deref()
                            .ok_or_else(|| {
                                Error::Deserialize("ARRAY type missing element type".into())
                            })?;
                    visitor.visit_seq(IntoArraySeqAccess {
                        values: list.values.into_iter(),
                        elem_type,
                    })
                }
                _ => Err(Error::Deserialize(
                    "expected list value for ARRAY column".into(),
                )),
            },
            pb::TypeCode::Struct => match kind {
                Kind::ListValue(list) => {
                    let struct_type = self.spanner_type.struct_type.as_ref().ok_or_else(|| {
                        Error::Deserialize("STRUCT type missing struct_type".into())
                    })?;
                    visitor.visit_map(IntoStructMapAccess {
                        fields: &struct_type.fields,
                        values: list.values.into_iter(),
                        index: 0,
                    })
                }
                _ => Err(Error::Deserialize(
                    "expected list value for STRUCT column".into(),
                )),
            },
            _ => Err(Error::Deserialize(format!(
                "unsupported type code: {code:?}"
            ))),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match &self.value.kind {
            None | Some(prost_types::value::Kind::NullValue(_)) => visitor.visit_none(),
            Some(_) => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf unit unit_struct seq tuple tuple_struct map struct enum
        identifier ignored_any
    }
}

struct IntoArraySeqAccess<'a> {
    values: std::vec::IntoIter<prost_types::Value>,
    elem_type: &'a pb::Type,
}

impl<'de, 'a> SeqAccess<'de> for IntoArraySeqAccess<'a> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        match self.values.next() {
            None => Ok(None),
            Some(val) => seed
                .deserialize(IntoValueDeserializer {
                    value: val,
                    spanner_type: self.elem_type,
                })
                .map(Some),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

struct IntoStructMapAccess<'a> {
    fields: &'a [pb::struct_type::Field],
    values: std::vec::IntoIter<prost_types::Value>,
    index: usize,
}

impl<'de, 'a> MapAccess<'de> for IntoStructMapAccess<'a> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        if self.index >= self.fields.len() {
            return Ok(None);
        }
        let key = self.fields[self.index].name.as_str();
        seed.deserialize(key.into_deserializer()).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let field = &self.fields[self.index];
        let val = self.values.next().ok_or_else(|| {
            Error::Deserialize(format!(
                "STRUCT field '{}' missing value at index {}",
                field.name, self.index
            ))
        })?;
        self.index += 1;
        let field_type = field.r#type.as_ref().ok_or_else(|| {
            Error::Deserialize(format!("STRUCT field '{}' missing type", field.name))
        })?;
        seed.deserialize(IntoValueDeserializer {
            value: val,
            spanner_type: field_type,
        })
    }
}

impl de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Deserialize(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::Deserialize;

    use crate::proto::google::spanner::v1 as pb;
    use crate::result::{ColumnMeta, Row};

    fn make_type(code: pb::TypeCode) -> pb::Type {
        pb::Type {
            code: code as i32,
            ..Default::default()
        }
    }

    fn string_val(s: &str) -> prost_types::Value {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(s.to_string())),
        }
    }

    fn number_val(n: f64) -> prost_types::Value {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(n)),
        }
    }

    fn bool_val(b: bool) -> prost_types::Value {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::BoolValue(b)),
        }
    }

    fn null_val() -> prost_types::Value {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        }
    }

    fn list_val(vals: Vec<prost_types::Value>) -> prost_types::Value {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue { values: vals },
            )),
        }
    }

    fn make_row(columns: Vec<ColumnMeta>, values: Vec<prost_types::Value>) -> Row {
        Row {
            columns: Arc::new(columns),
            values,
        }
    }

    #[test]
    fn deserialize_string() {
        #[derive(Deserialize, Debug)]
        struct R {
            name: String,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "name".into(),
                spanner_type: make_type(pb::TypeCode::String),
            }],
            vec![string_val("Alice")],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.name, "Alice");
    }

    #[test]
    fn deserialize_int64_from_string() {
        #[derive(Deserialize, Debug)]
        struct R {
            count: i64,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "count".into(),
                spanner_type: make_type(pb::TypeCode::Int64),
            }],
            vec![string_val("42")],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.count, 42);
    }

    #[test]
    fn deserialize_float64() {
        #[derive(Deserialize, Debug)]
        struct R {
            score: f64,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "score".into(),
                spanner_type: make_type(pb::TypeCode::Float64),
            }],
            vec![number_val(99.5)],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert!((r.score - 99.5).abs() < f64::EPSILON);
    }

    #[test]
    fn deserialize_bool() {
        #[derive(Deserialize, Debug)]
        struct R {
            active: bool,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "active".into(),
                spanner_type: make_type(pb::TypeCode::Bool),
            }],
            vec![bool_val(true)],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert!(r.active);
    }

    #[test]
    fn deserialize_option_some() {
        #[derive(Deserialize, Debug)]
        struct R {
            value: Option<i64>,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "value".into(),
                spanner_type: make_type(pb::TypeCode::Int64),
            }],
            vec![string_val("7")],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.value, Some(7));
    }

    #[test]
    fn deserialize_option_none() {
        #[derive(Deserialize, Debug)]
        struct R {
            value: Option<i64>,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "value".into(),
                spanner_type: make_type(pb::TypeCode::Int64),
            }],
            vec![null_val()],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.value, None);
    }

    #[test]
    fn deserialize_array() {
        #[derive(Deserialize, Debug)]
        struct R {
            tags: Vec<String>,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "tags".into(),
                spanner_type: pb::Type {
                    code: pb::TypeCode::Array as i32,
                    array_element_type: Some(Box::new(make_type(pb::TypeCode::String))),
                    ..Default::default()
                },
            }],
            vec![list_val(vec![string_val("a"), string_val("b")])],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.tags, vec!["a", "b"]);
    }

    #[test]
    fn deserialize_multiple_columns() {
        #[derive(Deserialize, Debug)]
        struct R {
            id: String,
            balance: i64,
            active: bool,
        }
        let row = make_row(
            vec![
                ColumnMeta {
                    name: "id".into(),
                    spanner_type: make_type(pb::TypeCode::String),
                },
                ColumnMeta {
                    name: "balance".into(),
                    spanner_type: make_type(pb::TypeCode::Int64),
                },
                ColumnMeta {
                    name: "active".into(),
                    spanner_type: make_type(pb::TypeCode::Bool),
                },
            ],
            vec![string_val("alice"), string_val("1000"), bool_val(true)],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.id, "alice");
        assert_eq!(r.balance, 1000);
        assert!(r.active);
    }

    #[test]
    fn deserialize_with_rename() {
        #[derive(Deserialize, Debug)]
        struct R {
            #[serde(rename = "UserName")]
            user_name: String,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "UserName".into(),
                spanner_type: make_type(pb::TypeCode::String),
            }],
            vec![string_val("Bob")],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.user_name, "Bob");
    }

    #[test]
    fn deserialize_nested_struct() {
        #[derive(Deserialize, Debug)]
        struct Inner {
            x: i64,
            y: String,
        }
        #[derive(Deserialize, Debug)]
        struct R {
            data: Inner,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "data".into(),
                spanner_type: pb::Type {
                    code: pb::TypeCode::Struct as i32,
                    struct_type: Some(pb::StructType {
                        fields: vec![
                            pb::struct_type::Field {
                                name: "x".into(),
                                r#type: Some(make_type(pb::TypeCode::Int64)),
                            },
                            pb::struct_type::Field {
                                name: "y".into(),
                                r#type: Some(make_type(pb::TypeCode::String)),
                            },
                        ],
                    }),
                    ..Default::default()
                },
            }],
            vec![list_val(vec![string_val("5"), string_val("hello")])],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.data.x, 5);
        assert_eq!(r.data.y, "hello");
    }

    #[test]
    fn deserialize_array_f32() {
        #[derive(Deserialize, Debug)]
        struct R {
            embedding: Vec<f32>,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "embedding".into(),
                spanner_type: pb::Type {
                    code: pb::TypeCode::Array as i32,
                    array_element_type: Some(Box::new(make_type(pb::TypeCode::Float32))),
                    ..Default::default()
                },
            }],
            vec![list_val(vec![
                number_val(1.0),
                number_val(2.5),
                number_val(3.0),
            ])],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.embedding, vec![1.0f32, 2.5, 3.0]);
    }

    #[test]
    fn deserialize_bytes_base64() {
        #[derive(Deserialize, Debug)]
        struct R {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = make_row(
            vec![ColumnMeta {
                name: "data".into(),
                spanner_type: make_type(pb::TypeCode::Bytes),
            }],
            // base64 of [1,2,3] = "AQID"
            vec![string_val("AQID")],
        );
        let r: R = row.deserialize().expect("row should deserialize");
        assert_eq!(r.data, vec![1, 2, 3]);
    }

    #[test]
    fn missing_column_value_returns_deserialize_error() {
        #[derive(Deserialize, Debug)]
        struct R {
            id: String,
            n: i64,
        }

        let row = make_row(
            vec![
                ColumnMeta {
                    name: "id".into(),
                    spanner_type: make_type(pb::TypeCode::String),
                },
                ColumnMeta {
                    name: "n".into(),
                    spanner_type: make_type(pb::TypeCode::Int64),
                },
            ],
            vec![string_val("only-one-value")],
        );

        let result = row.deserialize::<R>();
        if let Ok(value) = &result {
            // Keep fields observed for dead-code lint while preserving test intent.
            let _ = (&value.id, value.n);
        }
        let err = result.expect_err("row should be incomplete");
        assert!(
            err.to_string().contains("row missing value for column"),
            "unexpected error: {err}"
        );
    }
}
