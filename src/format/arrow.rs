use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, ListArray, StringArray, StructArray,
    UInt32Array, UInt8Array,
};

use super::{BatchLike, IcechunkFormatError, IcechunkResult};

//TODO: move these helper classes to a support file
pub struct ColumnIndex<'a, 'b> {
    pub column: &'a ArrayRef,
    pub name: &'b str,
}

pub fn get_column<'a, 'b /*: 'a*/>(
    batch: &'a impl BatchLike,
    column_name: &'b str,
) -> IcechunkResult<ColumnIndex<'a, 'b>> {
    Ok(ColumnIndex {
        column: batch.get_batch().column_by_name(column_name).ok_or(
            IcechunkFormatError::ColumnNotFound { column: column_name.to_string() },
        )?,
        name: column_name,
    })
}

impl<'a, 'b> ColumnIndex<'a, 'b> {
    pub fn as_string(&self) -> IcechunkResult<&'a StringArray> {
        self.column.as_string_opt().ok_or(IcechunkFormatError::InvalidColumnType {
            column_name: self.name.to_string(),
            expected_column_type: "string".to_string(),
        })
    }

    pub fn as_u32(&self) -> IcechunkResult<&'a UInt32Array> {
        self.column.as_primitive_opt().ok_or(IcechunkFormatError::InvalidColumnType {
            column_name: self.name.to_string(),
            expected_column_type: "u32".to_string(),
        })
    }

    pub fn as_u8(&self) -> IcechunkResult<&'a UInt8Array> {
        self.column.as_primitive_opt().ok_or(IcechunkFormatError::InvalidColumnType {
            column_name: self.name.to_string(),
            expected_column_type: "u8".to_string(),
        })
    }

    pub fn as_list(&self) -> IcechunkResult<&'a ListArray> {
        self.column.as_list_opt::<i32>().ok_or(IcechunkFormatError::InvalidColumnType {
            column_name: self.name.to_string(),
            expected_column_type: "list".to_string(),
        })
    }

    pub fn as_struct(&self) -> IcechunkResult<&'a StructArray> {
        self.column.as_struct_opt().ok_or(IcechunkFormatError::InvalidColumnType {
            column_name: self.name.to_string(),
            expected_column_type: "struct".to_string(),
        })
    }

    pub fn as_binary(&self) -> IcechunkResult<&'a BinaryArray> {
        self.column.as_binary_opt::<i32>().ok_or(IcechunkFormatError::InvalidColumnType {
            column_name: self.name.to_string(),
            expected_column_type: "binary".to_string(),
        })
    }

    pub fn string_at(&self, idx: usize) -> IcechunkResult<&'a str> {
        let array = self.as_string()?;
        if array.is_valid(idx) {
            Ok(array.value(idx))
        } else {
            Err(IcechunkFormatError::NullElement {
                index: idx,
                column_name: self.name.to_string(),
            })
        }
    }

    pub fn u32_at(&self, idx: usize) -> IcechunkResult<u32> {
        let array = self.as_u32()?;
        if array.is_valid(idx) {
            Ok(array.value(idx))
        } else {
            Err(IcechunkFormatError::NullElement {
                index: idx,
                column_name: self.name.to_string(),
            })
        }
    }

    pub fn u8_at(&self, idx: usize) -> IcechunkResult<u8> {
        let array = self.as_u8()?;
        if array.is_valid(idx) {
            Ok(array.value(idx))
        } else {
            Err(IcechunkFormatError::NullElement {
                index: idx,
                column_name: self.name.to_string(),
            })
        }
    }

    pub fn binary_at(&self, idx: usize) -> IcechunkResult<&'a [u8]> {
        let array = self.as_binary()?;
        if array.is_valid(idx) {
            Ok(array.value(idx))
        } else {
            Err(IcechunkFormatError::NullElement {
                index: idx,
                column_name: self.name.to_string(),
            })
        }
    }

    pub fn list_at(&self, idx: usize) -> IcechunkResult<ArrayRef> {
        let array = self.as_list()?;
        if array.is_valid(idx) {
            Ok(array.value(idx))
        } else {
            Err(IcechunkFormatError::NullElement {
                index: idx,
                column_name: self.name.to_string(),
            })
        }
    }

    pub fn list_at_opt(&self, idx: usize) -> IcechunkResult<Option<ArrayRef>> {
        let array = self.as_list()?;
        if array.is_valid(idx) {
            Ok(Some(array.value(idx)))
        } else {
            Ok(None)
        }
    }
}
