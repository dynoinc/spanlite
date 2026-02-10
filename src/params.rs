use std::collections::{BTreeMap, HashMap};

use crate::proto::google::spanner::v1 as pb;

fn scalar_type(code: pb::TypeCode) -> pb::Type {
    pb::Type {
        code: code as i32,
        ..Default::default()
    }
}

/// Trait for converting Rust values to Spanner-typed protobuf values.
///
/// Implementations produce a `(Value, Type)` pair that correctly encodes
/// the value according to Spanner's wire format (e.g. INT64 as a string).
pub trait ToSpanner: Sync {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type);

    fn spanner_type() -> pb::Type
    where
        Self: Sized;
}

impl ToSpanner for i64 {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        scalar_type(pb::TypeCode::Int64)
    }
}

macro_rules! impl_to_spanner_via {
    ($target:ty; $($ty:ty),+ $(,)?) => {
        $(
            impl ToSpanner for $ty {
                fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
                    (*self as $target).to_spanner()
                }

                fn spanner_type() -> pb::Type {
                    <$target as ToSpanner>::spanner_type()
                }
            }
        )+
    };
}

impl_to_spanner_via!(i64; i32, u32, i16, u16);

// Note: u8 is intentionally omitted because Vec<u8> and &[u8] are
// specialized as BYTES (base64). Adding ToSpanner for u8 would conflict
// with the blanket Vec<T: ToSpanner> impl.

impl ToSpanner for f64 {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(*self)),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        scalar_type(pb::TypeCode::Float64)
    }
}

impl ToSpanner for f32 {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(*self as f64)),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        scalar_type(pb::TypeCode::Float32)
    }
}

impl ToSpanner for bool {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::BoolValue(*self)),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        scalar_type(pb::TypeCode::Bool)
    }
}

impl ToSpanner for &str {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        scalar_type(pb::TypeCode::String)
    }
}

impl ToSpanner for String {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        self.as_str().to_spanner()
    }

    fn spanner_type() -> pb::Type {
        <&str as ToSpanner>::spanner_type()
    }
}

impl ToSpanner for &[u8] {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self),
                )),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        scalar_type(pb::TypeCode::Bytes)
    }
}

impl ToSpanner for Vec<u8> {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        self.as_slice().to_spanner()
    }

    fn spanner_type() -> pb::Type {
        <&[u8] as ToSpanner>::spanner_type()
    }
}

impl<T: ToSpanner> ToSpanner for &[T] {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        let values: Vec<prost_types::Value> = self.iter().map(|v| v.to_spanner().0).collect();
        (
            prost_types::Value {
                kind: Some(prost_types::value::Kind::ListValue(
                    prost_types::ListValue { values },
                )),
            },
            Self::spanner_type(),
        )
    }

    fn spanner_type() -> pb::Type {
        pb::Type {
            code: pb::TypeCode::Array as i32,
            array_element_type: Some(Box::new(T::spanner_type())),
            ..Default::default()
        }
    }
}

impl<T: ToSpanner> ToSpanner for Vec<T> {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        self.as_slice().to_spanner()
    }

    fn spanner_type() -> pb::Type {
        <&[T] as ToSpanner>::spanner_type()
    }
}

impl<T: ToSpanner> ToSpanner for Option<T> {
    fn to_spanner(&self) -> (prost_types::Value, pb::Type) {
        match self {
            Some(v) => v.to_spanner(),
            None => (
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::NullValue(0)),
                },
                Self::spanner_type(),
            ),
        }
    }

    fn spanner_type() -> pb::Type {
        T::spanner_type()
    }
}

/// Convert a slice of `(&str, &dyn ToSpanner)` pairs into protobuf param structs.
pub(crate) fn params_to_proto(
    params: &[(&str, &dyn ToSpanner)],
) -> (prost_types::Struct, HashMap<String, pb::Type>) {
    let mut fields = BTreeMap::new();
    let mut param_types = HashMap::with_capacity(params.len());

    for (name, value) in params {
        let (val, ty) = value.to_spanner();
        fields.insert(name.to_string(), val);
        param_types.insert(name.to_string(), ty);
    }

    (prost_types::Struct { fields }, param_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::value::Kind;

    #[test]
    fn i64_encoded_as_string() {
        let (val, ty) = 42i64.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "42"));
    }

    #[test]
    fn f64_encoded_as_number() {
        let (val, ty) = 2.72f64.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Float64 as i32);
        assert!(matches!(val.kind, Some(Kind::NumberValue(n)) if (n - 2.72).abs() < f64::EPSILON));
    }

    #[test]
    fn f32_encoded_as_number() {
        let (val, ty) = 1.5f32.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Float32 as i32);
        assert!(matches!(val.kind, Some(Kind::NumberValue(n)) if (n - 1.5).abs() < f64::EPSILON));
    }

    #[test]
    fn bool_encoded() {
        let (val, ty) = true.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Bool as i32);
        assert!(matches!(val.kind, Some(Kind::BoolValue(true))));
    }

    #[test]
    fn str_encoded() {
        let (val, ty) = "hello".to_spanner();
        assert_eq!(ty.code, pb::TypeCode::String as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "hello"));
    }

    #[test]
    fn string_encoded() {
        let (val, ty) = String::from("world").to_spanner();
        assert_eq!(ty.code, pb::TypeCode::String as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "world"));
    }

    #[test]
    fn bytes_encoded_as_base64() {
        let data = vec![0u8, 1, 2, 3];
        let (val, ty) = data.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Bytes as i32);
        // base64 of [0,1,2,3] = "AAECAw=="
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "AAECAw=="));
    }

    #[test]
    fn slice_bytes_encoded_as_base64() {
        let data: &[u8] = &[0, 1, 2, 3];
        let (val, ty) = data.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Bytes as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "AAECAw=="));
    }

    #[test]
    fn option_some() {
        let (val, ty) = Some(10i64).to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "10"));
    }

    #[test]
    fn option_none() {
        let (val, ty) = Option::<i64>::None.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::NullValue(0))));
    }

    #[test]
    fn vec_f32_encoded_as_array() {
        let embedding = vec![1.0f32, 2.0, 3.0];
        let (val, ty) = embedding.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Array as i32);
        assert_eq!(
            ty.array_element_type
                .as_ref()
                .expect("ARRAY type should include element type")
                .code,
            pb::TypeCode::Float32 as i32
        );
        if let Some(Kind::ListValue(list)) = val.kind {
            assert_eq!(list.values.len(), 3);
            assert!(
                matches!(list.values[0].kind, Some(Kind::NumberValue(n)) if (n - 1.0).abs() < f64::EPSILON)
            );
        } else {
            panic!("expected list value");
        }
    }

    #[test]
    fn vec_i64_encoded_as_array() {
        let ids = vec![1i64, 2, 3];
        let (val, ty) = ids.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Array as i32);
        assert_eq!(
            ty.array_element_type
                .as_ref()
                .expect("ARRAY type should include element type")
                .code,
            pb::TypeCode::Int64 as i32
        );
        if let Some(Kind::ListValue(list)) = val.kind {
            assert_eq!(list.values.len(), 3);
            assert!(matches!(&list.values[0].kind, Some(Kind::StringValue(s)) if s == "1"));
        } else {
            panic!("expected list value");
        }
    }

    #[test]
    fn slice_f32_encoded_as_array() {
        let embedding: &[f32] = &[1.0, 2.0];
        let (val, ty) = embedding.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Array as i32);
        if let Some(Kind::ListValue(list)) = val.kind {
            assert_eq!(list.values.len(), 2);
        } else {
            panic!("expected list value");
        }
    }

    #[test]
    fn empty_vec_encoded_as_array() {
        let empty: Vec<f32> = vec![];
        let (val, ty) = empty.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Array as i32);
        assert_eq!(
            ty.array_element_type
                .as_ref()
                .expect("ARRAY type should include element type")
                .code,
            pb::TypeCode::Float32 as i32
        );
        if let Some(Kind::ListValue(list)) = val.kind {
            assert!(list.values.is_empty());
        } else {
            panic!("expected list value");
        }
    }

    #[test]
    fn i32_encoded_as_int64_string() {
        let (val, ty) = 42i32.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "42"));
    }

    #[test]
    fn u32_encoded_as_int64_string() {
        let (val, ty) = 42u32.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "42"));
    }

    #[test]
    fn i16_encoded_as_int64_string() {
        let (val, ty) = 42i16.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "42"));
    }

    #[test]
    fn u16_encoded_as_int64_string() {
        let (val, ty) = 42u16.to_spanner();
        assert_eq!(ty.code, pb::TypeCode::Int64 as i32);
        assert!(matches!(val.kind, Some(Kind::StringValue(s)) if s == "42"));
    }

    #[test]
    fn params_to_proto_basic() {
        let (s, types) = params_to_proto(&[("x", &42i64), ("y", &"hello")]);
        assert_eq!(s.fields.len(), 2);
        assert_eq!(types.len(), 2);
        assert_eq!(types["x"].code, pb::TypeCode::Int64 as i32);
        assert_eq!(types["y"].code, pb::TypeCode::String as i32);
    }
}
