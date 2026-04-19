use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema};

use crate::event::ComponentKind;

pub const ALL_COMPONENT_KINDS: [ComponentKind; 5] = ComponentKind::ALL;

pub const COL_T_NS: &str = "t_ns";
pub const COL_ENTITY: &str = "entity_path";

fn common_fields() -> Vec<Field> {
    vec![
        Field::new(COL_T_NS, DataType::Int64, false),
        Field::new(COL_ENTITY, DataType::Utf8, false),
    ]
}

pub fn schema_for(kind: ComponentKind) -> Arc<Schema> {
    let mut fields = common_fields();
    match kind {
        ComponentKind::Transform3D => {
            fields.extend([
                Field::new("tx", DataType::Float64, false),
                Field::new("ty", DataType::Float64, false),
                Field::new("tz", DataType::Float64, false),
                Field::new("qx", DataType::Float64, false),
                Field::new("qy", DataType::Float64, false),
                Field::new("qz", DataType::Float64, false),
                Field::new("qw", DataType::Float64, false),
            ]);
        }
        ComponentKind::Points3D => {
            let point_struct = DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
                Field::new("z", DataType::Float64, false),
            ]));
            fields.push(Field::new(
                "points",
                DataType::List(Arc::new(Field::new("item", point_struct, false))),
                false,
            ));
        }
        ComponentKind::Scalar => {
            fields.extend([
                Field::new("value", DataType::Float64, false),
                Field::new("unit", DataType::Utf8, true),
            ]);
        }
        ComponentKind::ImageRef => {
            fields.extend([
                Field::new("uri", DataType::Utf8, false),
                Field::new("width", DataType::Int32, false),
                Field::new("height", DataType::Int32, false),
                Field::new("format", DataType::Utf8, false),
            ]);
        }
        ComponentKind::Annotation => {
            fields.push(Field::new("label", DataType::Utf8, false));
        }
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_schema_starts_with_common_fields() {
        for kind in ALL_COMPONENT_KINDS {
            let schema = schema_for(kind);
            assert_eq!(schema.field(0).name(), COL_T_NS);
            assert_eq!(schema.field(1).name(), COL_ENTITY);
        }
    }

    #[test]
    fn schemas_are_distinct_per_kind() {
        let mut names = std::collections::BTreeSet::new();
        for kind in ALL_COMPONENT_KINDS {
            let schema = schema_for(kind);
            let signature: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect();
            let joined = signature.join(",");
            assert!(names.insert(joined), "duplicate schema for {kind:?}");
        }
    }
}
