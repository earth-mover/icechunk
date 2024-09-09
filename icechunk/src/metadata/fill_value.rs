use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::format::IcechunkFormatError;

use super::DataType;

#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
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
    RawBits(Vec<u8>),
}

impl FillValue {
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
            (DataType::Float32, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float32(
                    n.as_f64().expect("bug in from_data_type_and_json") as f32,
                ))
            }
            (DataType::Float64, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float64(
                    n.as_f64().expect("bug in from_data_type_and_json"),
                ))
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

            (DataType::RawBits(n), serde_json::Value::Array(arr)) if arr.len() == *n => {
                let bits = arr
                    .iter()
                    .map(|b| FillValue::from_data_type_and_json(&DataType::UInt8, b))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FillValue::RawBits(
                    bits.iter()
                        .map(|b| match b {
                            FillValue::UInt8(n) => *n,
                            _ => 0,
                        })
                        .collect(),
                ))
            }
            _ => Err(IcechunkFormatError::FillValueParse {
                data_type: dt.clone(),
                value: value.clone(),
            }),
        }
    }

    pub fn from_data_type_and_value(
        dt: &DataType,
        value: &[u8],
    ) -> Result<Self, IcechunkFormatError> {
        use IcechunkFormatError::FillValueDecodeError;

        match dt {
            DataType::Bool => {
                if value.len() != 1 {
                    Err(FillValueDecodeError {
                        found_size: value.len(),
                        target_size: 1,
                        target_type: DataType::Bool,
                    })
                } else {
                    Ok(FillValue::Bool(value[0] != 0))
                }
            }
            DataType::Int8 => value
                .try_into()
                .map(|x| FillValue::Int8(i8::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 1,
                    target_type: DataType::Int8,
                }),
            DataType::Int16 => value
                .try_into()
                .map(|x| FillValue::Int16(i16::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 2,
                    target_type: DataType::Int16,
                }),
            DataType::Int32 => value
                .try_into()
                .map(|x| FillValue::Int32(i32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::Int32,
                }),
            DataType::Int64 => value
                .try_into()
                .map(|x| FillValue::Int64(i64::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 8,
                    target_type: DataType::Int64,
                }),
            DataType::UInt8 => value
                .try_into()
                .map(|x| FillValue::UInt8(u8::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 1,
                    target_type: DataType::UInt8,
                }),
            DataType::UInt16 => value
                .try_into()
                .map(|x| FillValue::UInt16(u16::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 2,
                    target_type: DataType::UInt16,
                }),
            DataType::UInt32 => value
                .try_into()
                .map(|x| FillValue::UInt32(u32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::UInt32,
                }),
            DataType::UInt64 => value
                .try_into()
                .map(|x| FillValue::UInt64(u64::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 8,
                    target_type: DataType::UInt64,
                }),
            DataType::Float16 => value
                .try_into()
                .map(|x| FillValue::Float16(f32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::Float16,
                }),
            DataType::Float32 => value
                .try_into()
                .map(|x| FillValue::Float32(f32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::Float32,
                }),
            DataType::Float64 => value
                .try_into()
                .map(|x| FillValue::Float64(f64::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 8,
                    target_type: DataType::Float64,
                }),
            DataType::Complex64 => {
                if value.len() != 8 {
                    Err(FillValueDecodeError {
                        found_size: value.len(),
                        target_size: 8,
                        target_type: DataType::Complex64,
                    })
                } else {
                    let r = value[..4].try_into().map(f32::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 4,
                            target_type: DataType::Complex64,
                        }
                    });
                    let i = value[4..].try_into().map(f32::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 4,
                            target_type: DataType::Complex64,
                        }
                    });
                    Ok(FillValue::Complex64(r?, i?))
                }
            }
            DataType::Complex128 => {
                if value.len() != 16 {
                    Err(FillValueDecodeError {
                        found_size: value.len(),
                        target_size: 16,
                        target_type: DataType::Complex128,
                    })
                } else {
                    let r = value[..8].try_into().map(f64::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 8,
                            target_type: DataType::Complex128,
                        }
                    });
                    let i = value[8..].try_into().map(f64::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 8,
                            target_type: DataType::Complex128,
                        }
                    });
                    Ok(FillValue::Complex128(r?, i?))
                }
            }
            DataType::RawBits(_) => Ok(FillValue::RawBits(value.to_owned())),
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
            FillValue::RawBits(v) => DataType::RawBits(v.len()),
        }
    }

    pub fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            FillValue::Bool(v) => vec![if v.to_owned() { 1 } else { 0 }],
            FillValue::Int8(v) => v.to_be_bytes().into(),
            FillValue::Int16(v) => v.to_be_bytes().into(),
            FillValue::Int32(v) => v.to_be_bytes().into(),
            FillValue::Int64(v) => v.to_be_bytes().into(),
            FillValue::UInt8(v) => v.to_be_bytes().into(),
            FillValue::UInt16(v) => v.to_be_bytes().into(),
            FillValue::UInt32(v) => v.to_be_bytes().into(),
            FillValue::UInt64(v) => v.to_be_bytes().into(),
            FillValue::Float16(v) => v.to_be_bytes().into(),
            FillValue::Float32(v) => v.to_be_bytes().into(),
            FillValue::Float64(v) => v.to_be_bytes().into(),
            FillValue::Complex64(r, i) => {
                r.to_be_bytes().into_iter().chain(i.to_be_bytes()).collect()
            }
            FillValue::Complex128(r, i) => {
                r.to_be_bytes().into_iter().chain(i.to_be_bytes()).collect()
            }
            FillValue::RawBits(v) => v.to_owned(),
        }
    }
}
