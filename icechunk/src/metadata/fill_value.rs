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
    RawBits(Vec<u8>),
}

impl FillValue {
    pub fn from_data_type_and_untagged(
        dt: &DataType,
        fill_value: FillValue,
    ) -> Result<Self, IcechunkFormatError> {
        #![allow(clippy::expect_used)] // before calling `as_foo` we check with `fits_foo`
        use FillValue::*;
        match (dt, &fill_value) {
            (DataType::Bool, Bool(_)) => Ok(fill_value),
            (DataType::Int8, Int8(_)) => Ok(fill_value),
            (DataType::Int16, Int16(_)) => Ok(fill_value),
            (DataType::Int32, Int32(_)) => Ok(fill_value),
            (DataType::Int64, Int64(_)) => Ok(fill_value),
            (DataType::UInt8, UInt8(_)) => Ok(fill_value),
            (DataType::UInt16, UInt16(_)) => Ok(fill_value),
            (DataType::UInt32, UInt32(_)) => Ok(fill_value),
            (DataType::UInt64, UInt64(_)) => Ok(fill_value),
            (DataType::Float16, Float16(_)) => Ok(fill_value),
            (DataType::Float32, Float32(_)) => Ok(fill_value),
            (DataType::Float64, Float64(_)) => Ok(fill_value),
            (DataType::Complex64, Complex64(_, _)) => Ok(fill_value),
            (DataType::Complex128, Complex128(_, _)) => Ok(fill_value),
            (DataType::RawBits(_), RawBits(_)) => Ok(fill_value),

            (DataType::Int16, Int8(n)) => Ok(Int16(*n as i16)),
            (DataType::Int32, Int8(n)) => Ok(Int32(*n as i32)),
            (DataType::Int32, Int16(n)) => Ok(Int32(*n as i32)),
            (DataType::Int64, Int8(n)) => Ok(Int64(*n as i64)),
            (DataType::Int64, Int16(n)) => Ok(Int64(*n as i64)),
            (DataType::Int64, Int32(n)) => Ok(Int64(*n as i64)),

            (DataType::UInt16, UInt8(n)) => Ok(UInt16(*n as u16)),
            (DataType::UInt32, UInt8(n)) => Ok(UInt32(*n as u32)),
            (DataType::UInt32, UInt16(n)) => Ok(UInt32(*n as u32)),
            (DataType::UInt64, UInt8(n)) => Ok(UInt64(*n as u64)),
            (DataType::UInt64, UInt16(n)) => Ok(UInt64(*n as u64)),
            (DataType::UInt64, UInt32(n)) => Ok(UInt64(*n as u64)),

            (DataType::Float32, Float16(n)) => Ok(Float32(*n)),
            (DataType::Float64, Float16(n)) => Ok(Float64(*n as f64)),
            (DataType::Float64, Float32(n)) => Ok(Float64(*n as f64)),

            // TODO: ranges
            (DataType::RawBits(_), Complex64(r, i)) => {
                Ok(RawBits(vec![*r as u8, *i as u8]))
            }

            // TODO: ranges
            (DataType::RawBits(_), Complex128(r, i)) => {
                Ok(RawBits(vec![*r as u8, *i as u8]))
            }
            _ => Err(IcechunkFormatError::FillValueParse {
                data_type: dt.clone(),
                value: fill_value.clone(),
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
