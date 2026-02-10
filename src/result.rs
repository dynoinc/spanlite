use std::collections::VecDeque;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::de::IntoRowDeserializer;
use crate::error::Error;
use crate::proto::google::spanner::v1 as pb;

/// Column metadata shared across all rows in a result set.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub spanner_type: pb::Type,
}

/// A single row of Spanner data.
///
/// Columns are shared via Arc (allocated once per result set).
/// Values are owned per row.
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Arc<Vec<ColumnMeta>>,
    pub values: Vec<prost_types::Value>,
}

impl Row {
    /// Deserialize this row into a user-defined struct by consuming it.
    ///
    /// Moves owned strings instead of cloning, avoiding allocations on the
    /// deserialization path.
    pub fn deserialize<T: DeserializeOwned>(self) -> Result<T, Error> {
        let de = IntoRowDeserializer::new(self.columns, self.values);
        T::deserialize(de)
    }
}

/// Assembles `PartialResultSet` messages from a streaming RPC into `Row`s.
///
/// Handles:
/// - `chunked_value` merging (strings concatenate, lists merge last/first recursively)
/// - Row boundary detection (emit row every `num_columns` values)
/// - `resume_token` tracking
pub struct StreamingAssembler {
    columns: Option<Arc<Vec<ColumnMeta>>>,
    num_columns: usize,
    pending_values: VecDeque<prost_types::Value>,
    /// When the previous PartialResultSet had chunked_value=true, this holds
    /// the incomplete trailing value to merge with the next message's first value.
    chunked_tail: Option<prost_types::Value>,
    pub resume_token: Vec<u8>,
}

impl Default for StreamingAssembler {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingAssembler {
    pub fn new() -> Self {
        Self {
            columns: None,
            num_columns: 0,
            pending_values: VecDeque::new(),
            chunked_tail: None,
            resume_token: Vec::new(),
        }
    }

    /// Process one PartialResultSet and return any complete rows.
    pub fn push(&mut self, partial: pb::PartialResultSet) -> Result<Vec<Row>, Error> {
        let pb::PartialResultSet {
            metadata,
            mut values,
            chunked_value,
            resume_token,
            ..
        } = partial;

        // Extract metadata from the first message
        if self.columns.is_none()
            && let Some(metadata) = &metadata
            && let Some(row_type) = &metadata.row_type
        {
            let cols: Vec<ColumnMeta> = row_type
                .fields
                .iter()
                .map(|f| ColumnMeta {
                    name: f.name.clone(),
                    spanner_type: f.r#type.clone().unwrap_or_default(),
                })
                .collect();
            self.num_columns = cols.len();
            self.columns = Some(Arc::new(cols));
        }

        // Track resume_token (move, not clone)
        if !resume_token.is_empty() {
            self.resume_token = resume_token;
        }

        // Merge chunked_tail with first value if present
        if let Some(tail) = self.chunked_tail.take() {
            if let Some(first) = values.first_mut() {
                *first = merge_values(tail, std::mem::take(first));
            } else {
                self.chunked_tail = Some(tail);
            }
        }

        // If this message is chunked, pop the last value as the new tail
        if chunked_value && let Some(last) = values.pop() {
            self.chunked_tail = Some(last);
        }

        // Emit complete rows
        let mut rows = Vec::new();

        if self.num_columns > 0 {
            let Some(columns) = self.columns.as_ref().cloned() else {
                return Err(Error::Deserialize(
                    "row metadata missing columns while values are present".to_string(),
                ));
            };

            if self.pending_values.is_empty() {
                // Fast path: drain Vec directly, skip VecDeque
                let mut iter = values.into_iter();
                while iter.len() >= self.num_columns {
                    rows.push(Row {
                        columns: columns.clone(),
                        values: iter.by_ref().take(self.num_columns).collect(),
                    });
                }
                // Only leftover values (< num_columns) go into VecDeque
                self.pending_values.extend(iter);
            } else {
                // Slow path: merge with pending values from previous partials
                self.pending_values.extend(values);
                while self.pending_values.len() >= self.num_columns {
                    let mut row_values = Vec::with_capacity(self.num_columns);
                    for _ in 0..self.num_columns {
                        if let Some(value) = self.pending_values.pop_front() {
                            row_values.push(value);
                        }
                    }
                    rows.push(Row {
                        columns: columns.clone(),
                        values: row_values,
                    });
                }
            }
        }

        Ok(rows)
    }
}

/// Merge two chunked values according to Spanner's merging rules:
/// - Strings: concatenate
/// - Lists: merge last element of left with first element of right, then concat
/// - Other: right replaces left (shouldn't happen per spec)
pub(crate) fn merge_values(
    left: prost_types::Value,
    right: prost_types::Value,
) -> prost_types::Value {
    use prost_types::value::Kind;

    match (left.kind, right.kind) {
        (Some(Kind::StringValue(mut a)), Some(Kind::StringValue(b))) => {
            a.push_str(&b);
            prost_types::Value {
                kind: Some(Kind::StringValue(a)),
            }
        }
        (Some(Kind::ListValue(mut a)), Some(Kind::ListValue(b))) => {
            // Merge last element of a with first element of b, then concat rest
            if !a.values.is_empty() && !b.values.is_empty() {
                let a_last = a.values.pop().unwrap();
                let mut b_iter = b.values.into_iter();
                let b_first = b_iter.next().unwrap();
                a.values.reserve(1 + b_iter.len());
                a.values.push(merge_values(a_last, b_first));
                a.values.extend(b_iter);
            } else {
                a.values.extend(b.values);
            }
            prost_types::Value {
                kind: Some(Kind::ListValue(a)),
            }
        }
        (_, right_kind) => prost_types::Value { kind: right_kind },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::value::Kind;
    use serde::Deserialize;

    fn make_type(code: pb::TypeCode) -> pb::Type {
        pb::Type {
            code: code as i32,
            ..Default::default()
        }
    }

    fn string_val(s: &str) -> prost_types::Value {
        prost_types::Value {
            kind: Some(Kind::StringValue(s.to_string())),
        }
    }

    fn list_val(vals: Vec<prost_types::Value>) -> prost_types::Value {
        prost_types::Value {
            kind: Some(Kind::ListValue(prost_types::ListValue { values: vals })),
        }
    }

    #[test]
    fn merge_strings() {
        let merged = merge_values(string_val("foo"), string_val("bar"));
        assert!(matches!(merged.kind, Some(Kind::StringValue(s)) if s == "foobar"));
    }

    #[test]
    fn merge_lists_simple() {
        let a = list_val(vec![string_val("1"), string_val("2")]);
        let b = list_val(vec![string_val("3")]);
        let merged = merge_values(a, b);
        // Last of a ("2") merged with first of b ("3") → "23"
        // Result: ["1", "23"]
        if let Some(Kind::ListValue(list)) = merged.kind {
            assert_eq!(list.values.len(), 2);
            assert!(matches!(&list.values[0].kind, Some(Kind::StringValue(s)) if s == "1"));
            assert!(matches!(&list.values[1].kind, Some(Kind::StringValue(s)) if s == "23"));
        } else {
            panic!("expected list");
        }
    }

    #[test]
    fn assembler_single_partial() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct R {
            id: String,
            n: i64,
        }

        let mut asm = StreamingAssembler::new();
        let partial = pb::PartialResultSet {
            metadata: Some(pb::ResultSetMetadata {
                row_type: Some(pb::StructType {
                    fields: vec![
                        pb::struct_type::Field {
                            name: "id".into(),
                            r#type: Some(make_type(pb::TypeCode::String)),
                        },
                        pb::struct_type::Field {
                            name: "n".into(),
                            r#type: Some(make_type(pb::TypeCode::Int64)),
                        },
                    ],
                }),
                ..Default::default()
            }),
            values: vec![
                string_val("a"),
                string_val("1"),
                string_val("b"),
                string_val("2"),
            ],
            chunked_value: false,
            resume_token: vec![1, 2, 3],
            ..Default::default()
        };

        let rows = asm
            .push(partial)
            .expect("single partial should assemble into rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(asm.resume_token, vec![1, 2, 3]);

        let mut rows = rows.into_iter();
        let r0: R = rows
            .next()
            .unwrap()
            .deserialize()
            .expect("first row should deserialize");
        assert_eq!(
            r0,
            R {
                id: "a".into(),
                n: 1
            }
        );

        let r1: R = rows
            .next()
            .unwrap()
            .deserialize()
            .expect("second row should deserialize");
        assert_eq!(
            r1,
            R {
                id: "b".into(),
                n: 2
            }
        );
    }

    #[test]
    fn assembler_split_across_partials() {
        let mut asm = StreamingAssembler::new();

        // First partial: metadata + 1 value
        let p1 = pb::PartialResultSet {
            metadata: Some(pb::ResultSetMetadata {
                row_type: Some(pb::StructType {
                    fields: vec![
                        pb::struct_type::Field {
                            name: "x".into(),
                            r#type: Some(make_type(pb::TypeCode::String)),
                        },
                        pb::struct_type::Field {
                            name: "y".into(),
                            r#type: Some(make_type(pb::TypeCode::String)),
                        },
                    ],
                }),
                ..Default::default()
            }),
            values: vec![string_val("a")],
            chunked_value: false,
            ..Default::default()
        };

        let rows = asm.push(p1).expect("first partial should parse");
        assert_eq!(rows.len(), 0); // incomplete row

        // Second partial: remaining value
        let p2 = pb::PartialResultSet {
            values: vec![string_val("b")],
            chunked_value: false,
            ..Default::default()
        };

        let mut rows = asm.push(p2).expect("second partial should parse");
        assert_eq!(rows.len(), 1);

        #[derive(Deserialize, Debug)]
        struct R {
            x: String,
            y: String,
        }
        let r: R = rows
            .remove(0)
            .deserialize()
            .expect("assembled row should deserialize");
        assert_eq!(r.x, "a");
        assert_eq!(r.y, "b");
    }

    #[test]
    fn assembler_chunked_value() {
        let mut asm = StreamingAssembler::new();

        let p1 = pb::PartialResultSet {
            metadata: Some(pb::ResultSetMetadata {
                row_type: Some(pb::StructType {
                    fields: vec![pb::struct_type::Field {
                        name: "msg".into(),
                        r#type: Some(make_type(pb::TypeCode::String)),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![string_val("hel")],
            chunked_value: true, // last value is chunked
            ..Default::default()
        };

        let rows = asm.push(p1).expect("chunked first partial should parse");
        assert_eq!(rows.len(), 0);

        let p2 = pb::PartialResultSet {
            values: vec![string_val("lo")],
            chunked_value: false,
            ..Default::default()
        };

        let mut rows = asm.push(p2).expect("chunked second partial should parse");
        assert_eq!(rows.len(), 1);

        #[derive(Deserialize, Debug)]
        struct R {
            msg: String,
        }
        let r: R = rows
            .remove(0)
            .deserialize()
            .expect("chunked row should deserialize");
        assert_eq!(r.msg, "hello");
    }
}
