use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Complex64,
    Complex128,
    String,
    Bytes,
}

impl DataType {
    pub fn fits_i64(&self, n: i64) -> bool {
        use DataType::*;
        match self {
            Int8 => n >= i8::MIN as i64 && n <= i8::MAX as i64,
            Int16 => n >= i16::MIN as i64 && n <= i16::MAX as i64,
            Int32 => n >= i32::MIN as i64 && n <= i32::MAX as i64,
            Int64 => true,
            _ => false,
        }
    }

    pub fn fits_u64(&self, n: u64) -> bool {
        use DataType::*;
        match self {
            UInt8 => n >= u8::MIN as u64 && n <= u8::MAX as u64,
            UInt16 => n >= u16::MIN as u64 && n <= u16::MAX as u64,
            UInt32 => n >= u32::MIN as u64 && n <= u32::MAX as u64,
            UInt64 => true,
            _ => false,
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "bool" => Ok(DataType::Bool),
            "int8" => Ok(DataType::Int8),
            "int16" => Ok(DataType::Int16),
            "int32" => Ok(DataType::Int32),
            "int64" => Ok(DataType::Int64),
            "uint8" => Ok(DataType::UInt8),
            "uint16" => Ok(DataType::UInt16),
            "uint32" => Ok(DataType::UInt32),
            "uint64" => Ok(DataType::UInt64),
            "float16" => Ok(DataType::Float16),
            "float32" => Ok(DataType::Float32),
            "float64" => Ok(DataType::Float64),
            "complex64" => Ok(DataType::Complex64),
            "complex128" => Ok(DataType::Complex128),
            "string" => Ok(DataType::String),
            "bytes" => Ok(DataType::Bytes),
            _ => Err("Unknown data type, cannot parse"),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DataType::*;
        match self {
            Bool => f.write_str("bool"),
            Int8 => f.write_str("int8"),
            Int16 => f.write_str("int16"),
            Int32 => f.write_str("int32"),
            Int64 => f.write_str("int64"),
            UInt8 => f.write_str("uint8"),
            UInt16 => f.write_str("uint16"),
            UInt32 => f.write_str("uint32"),
            UInt64 => f.write_str("uint64"),
            Float16 => f.write_str("float16"),
            Float32 => f.write_str("float32"),
            Float64 => f.write_str("float64"),
            Complex64 => f.write_str("complex64"),
            Complex128 => f.write_str("complex128"),
            String => f.write_str("string"),
            Bytes => f.write_str("bytes"),
        }
    }
}
