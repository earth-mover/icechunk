use itertools::Itertools;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::format::IcechunkFormatError;

use super::DataType;

#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FillValue {
    // FIXME: test all json (de)serializations
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float16(f32),
    Float32(f32),
    Float64(f64),
    Complex64(f32, f32),
    Complex128(f64, f64),
    String(String),
    Bytes(Vec<u8>),
}

impl FillValue {
    pub const NAN_STR: &'static str = "NaN";
    pub const INF_STR: &'static str = "Infinity";
    pub const NEG_INF_STR: &'static str = "-Infinity";

    pub fn from_data_type_and_json(
        dt: &DataType,
        value: &serde_json::Value,
    ) -> Result<Self, IcechunkFormatError> {
        #![allow(clippy::expect_used)] // before calling `as_foo` we check with `fits_foo`
        match (dt, value) {
            (DataType::Bool, serde_json::Value::Bool(b)) => Ok(FillValue::Bool(*b)),
            (DataType::Int8, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int8(
                    n.as_i64().expect("bug in from_data_type_and_json") as i8
                ))
            }
            (DataType::Int16, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int16(
                    n.as_i64().expect("bug in from_data_type_and_json") as i16
                ))
            }
            (DataType::Int32, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int32(
                    n.as_i64().expect("bug in from_data_type_and_json") as i32
                ))
            }
            (DataType::Int64, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int64(n.as_i64().expect("bug in from_data_type_and_json")))
            }
            (DataType::UInt8, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt8(
                    n.as_u64().expect("bug in from_data_type_and_json") as u8
                ))
            }
            (DataType::UInt16, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt16(
                    n.as_u64().expect("bug in from_data_type_and_json") as u16
                ))
            }
            (DataType::UInt32, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt32(
                    n.as_u64().expect("bug in from_data_type_and_json") as u32
                ))
            }
            (DataType::UInt64, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt64(n.as_u64().expect("bug in from_data_type_and_json")))
            }
            (DataType::Float16, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float16(
                    n.as_f64().expect("bug in from_data_type_and_json") as f32,
                ))
            }
            (DataType::Float16, serde_json::Value::String(s))
                if s.as_str() == FillValue::NAN_STR =>
            {
                Ok(FillValue::Float16(f32::NAN))
            }
            (DataType::Float16, serde_json::Value::String(s))
                if s.as_str() == FillValue::INF_STR =>
            {
                Ok(FillValue::Float16(f32::INFINITY))
            }
            (DataType::Float16, serde_json::Value::String(s))
                if s.as_str() == FillValue::NEG_INF_STR =>
            {
                Ok(FillValue::Float16(f32::NEG_INFINITY))
            }

            (DataType::Float32, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float32(
                    n.as_f64().expect("bug in from_data_type_and_json") as f32,
                ))
            }
            (DataType::Float32, serde_json::Value::String(s))
                if s.as_str() == FillValue::NAN_STR =>
            {
                Ok(FillValue::Float32(f32::NAN))
            }
            (DataType::Float32, serde_json::Value::String(s))
                if s.as_str() == FillValue::INF_STR =>
            {
                Ok(FillValue::Float32(f32::INFINITY))
            }
            (DataType::Float32, serde_json::Value::String(s))
                if s.as_str() == FillValue::NEG_INF_STR =>
            {
                Ok(FillValue::Float32(f32::NEG_INFINITY))
            }

            (DataType::Float64, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float64(
                    n.as_f64().expect("bug in from_data_type_and_json"),
                ))
            }
            (DataType::Float64, serde_json::Value::String(s))
                if s.as_str() == FillValue::NAN_STR =>
            {
                Ok(FillValue::Float64(f64::NAN))
            }
            (DataType::Float64, serde_json::Value::String(s))
                if s.as_str() == FillValue::INF_STR =>
            {
                Ok(FillValue::Float64(f64::INFINITY))
            }
            (DataType::Float64, serde_json::Value::String(s))
                if s.as_str() == FillValue::NEG_INF_STR =>
            {
                Ok(FillValue::Float64(f64::NEG_INFINITY))
            }
            (DataType::Complex64, serde_json::Value::Array(arr)) if arr.len() == 2 => {
                let r = FillValue::from_data_type_and_json(&DataType::Float32, &arr[0])?;
                let i = FillValue::from_data_type_and_json(&DataType::Float32, &arr[1])?;
                match (r, i) {
                    (FillValue::Float32(r), FillValue::Float32(i)) => {
                        Ok(FillValue::Complex64(r, i))
                    }
                    _ => Err(IcechunkFormatError::FillValueParse {
                        data_type: dt.clone(),
                        value: value.clone(),
                    }),
                }
            }
            (DataType::Complex128, serde_json::Value::Array(arr)) if arr.len() == 2 => {
                let r = FillValue::from_data_type_and_json(&DataType::Float64, &arr[0])?;
                let i = FillValue::from_data_type_and_json(&DataType::Float64, &arr[1])?;
                match (r, i) {
                    (FillValue::Float64(r), FillValue::Float64(i)) => {
                        Ok(FillValue::Complex128(r, i))
                    }
                    _ => Err(IcechunkFormatError::FillValueParse {
                        data_type: dt.clone(),
                        value: value.clone(),
                    }),
                }
            }

            (DataType::String, serde_json::Value::String(s)) => {
                Ok(FillValue::String(s.clone()))
            }

            (DataType::Bytes, serde_json::Value::Array(arr)) => {
                let bytes = arr
                    .iter()
                    .map(|b| FillValue::from_data_type_and_json(&DataType::UInt8, b))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FillValue::Bytes(
                    bytes
                        .iter()
                        .map(|b| match b {
                            FillValue::UInt8(n) => Ok(*n),
                            _ => Err(IcechunkFormatError::FillValueParse {
                                data_type: dt.clone(),
                                value: value.clone(),
                            }),
                        })
                        .try_collect()?,
                ))
            }

            _ => Err(IcechunkFormatError::FillValueParse {
                data_type: dt.clone(),
                value: value.clone(),
            }),
        }
    }

    pub fn get_data_type(&self) -> DataType {
        match self {
            FillValue::Bool(_) => DataType::Bool,
            FillValue::Int8(_) => DataType::Int8,
            FillValue::Int16(_) => DataType::Int16,
            FillValue::Int32(_) => DataType::Int32,
            FillValue::Int64(_) => DataType::Int64,
            FillValue::UInt8(_) => DataType::UInt8,
            FillValue::UInt16(_) => DataType::UInt16,
            FillValue::UInt32(_) => DataType::UInt32,
            FillValue::UInt64(_) => DataType::UInt64,
            FillValue::Float16(_) => DataType::Float16,
            FillValue::Float32(_) => DataType::Float32,
            FillValue::Float64(_) => DataType::Float64,
            FillValue::Complex64(_, _) => DataType::Complex64,
            FillValue::Complex128(_, _) => DataType::Complex128,
            FillValue::String(_) => DataType::String,
            FillValue::Bytes(_) => DataType::Bytes,
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_nan_inf_parsing() {
        assert_eq!(
            FillValue::from_data_type_and_json(&DataType::Float64, &55f64.into())
                .unwrap(),
            FillValue::Float64(55.0)
        );

        assert_eq!(
            FillValue::from_data_type_and_json(&DataType::Float64, &55.into()).unwrap(),
            FillValue::Float64(55.0)
        );

        assert!(matches!(FillValue::from_data_type_and_json(
            &DataType::Float64,
            &"NaN".into()
        )
        .unwrap(),
            FillValue::Float64(n) if n.is_nan()
        ));

        assert!(matches!(FillValue::from_data_type_and_json(
            &DataType::Float64,
            &"Infinity".into()
        )
        .unwrap(),
            FillValue::Float64(n) if n == f64::INFINITY
        ));

        assert!(matches!(FillValue::from_data_type_and_json(
            &DataType::Float64,
            &"-Infinity".into()
        )
        .unwrap(),
            FillValue::Float64(n) if n == f64::NEG_INFINITY
        ));
    }
}
