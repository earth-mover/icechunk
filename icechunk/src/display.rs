#[macro_export]
macro_rules! write_dataclass_repr {
    // Add description of output here

    // Writes the class name
    ($f:expr, $class_name:literal) => {
        write!($f, "<{}>", $class_name)
    }; // Writes the specified attributes
       // ($f:expr, $class_name:literal, $($field_name:literal: $field_value:expr),+ $(,)?) => {
       //     write!($f, "<{}>\n{}", $class_name,
       //            [$((concat!($field_name, ": {}"), $field_value)),+]
       //            .iter()
       //            .map(|(fmt, val)| format!(fmt, val))
       //            .collect::<Vec<_>>()
       //            .join("\n"))
       //};
}
