use std::sync::Arc;

use anyhow::{bail, Result};
use arrow_array::{
    builder::{
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    },
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Fields, SchemaRef};
use mqd_core::{schema_for, ComponentKind, Event, Payload};

/// A per-kind row builder that accumulates rows and emits a single Arrow `RecordBatch` on drain.
pub enum KindBatcher {
    Transform3D(Transform3DBatcher),
    Points3D(Points3DBatcher),
    Scalar(ScalarBatcher),
    ImageRef(ImageRefBatcher),
    Annotation(AnnotationBatcher),
}

impl KindBatcher {
    pub fn new(kind: ComponentKind) -> Self {
        match kind {
            ComponentKind::Transform3D => Self::Transform3D(Transform3DBatcher::new()),
            ComponentKind::Points3D => Self::Points3D(Points3DBatcher::new()),
            ComponentKind::Scalar => Self::Scalar(ScalarBatcher::new()),
            ComponentKind::ImageRef => Self::ImageRef(ImageRefBatcher::new()),
            ComponentKind::Annotation => Self::Annotation(AnnotationBatcher::new()),
        }
    }

    pub fn kind(&self) -> ComponentKind {
        match self {
            Self::Transform3D(_) => ComponentKind::Transform3D,
            Self::Points3D(_) => ComponentKind::Points3D,
            Self::Scalar(_) => ComponentKind::Scalar,
            Self::ImageRef(_) => ComponentKind::ImageRef,
            Self::Annotation(_) => ComponentKind::Annotation,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Transform3D(b) => b.len,
            Self::Points3D(b) => b.len,
            Self::Scalar(b) => b.len,
            Self::ImageRef(b) => b.len,
            Self::Annotation(b) => b.len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, event: &Event) -> Result<()> {
        match (self, &event.payload) {
            (
                Self::Transform3D(b),
                Payload::Transform3D {
                    tx,
                    ty,
                    tz,
                    qx,
                    qy,
                    qz,
                    qw,
                },
            ) => {
                b.append(
                    event.t_ns,
                    event.entity_path.as_str(),
                    *tx,
                    *ty,
                    *tz,
                    *qx,
                    *qy,
                    *qz,
                    *qw,
                );
                Ok(())
            }
            (Self::Points3D(b), Payload::Points3D { points }) => {
                b.append(event.t_ns, event.entity_path.as_str(), points);
                Ok(())
            }
            (Self::Scalar(b), Payload::Scalar { value, unit }) => {
                b.append(
                    event.t_ns,
                    event.entity_path.as_str(),
                    *value,
                    unit.as_deref(),
                );
                Ok(())
            }
            (
                Self::ImageRef(b),
                Payload::ImageRef {
                    uri,
                    width,
                    height,
                    format,
                },
            ) => {
                b.append(
                    event.t_ns,
                    event.entity_path.as_str(),
                    uri,
                    *width,
                    *height,
                    format,
                );
                Ok(())
            }
            (Self::Annotation(b), Payload::Annotation { label }) => {
                b.append(event.t_ns, event.entity_path.as_str(), label);
                Ok(())
            }
            (this, payload) => bail!(
                "batcher kind mismatch: batcher={:?} event_kind={:?}",
                this.kind(),
                payload.kind()
            ),
        }
    }

    pub fn drain(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }
        let batch = match self {
            Self::Transform3D(b) => b.finish()?,
            Self::Points3D(b) => b.finish()?,
            Self::Scalar(b) => b.finish()?,
            Self::ImageRef(b) => b.finish()?,
            Self::Annotation(b) => b.finish()?,
        };
        Ok(Some(batch))
    }
}

pub struct Transform3DBatcher {
    schema: SchemaRef,
    t_ns: Int64Builder,
    entity: StringBuilder,
    tx: Float64Builder,
    ty: Float64Builder,
    tz: Float64Builder,
    qx: Float64Builder,
    qy: Float64Builder,
    qz: Float64Builder,
    qw: Float64Builder,
    len: usize,
}

impl Transform3DBatcher {
    fn new() -> Self {
        Self {
            schema: schema_for(ComponentKind::Transform3D),
            t_ns: Int64Builder::new(),
            entity: StringBuilder::new(),
            tx: Float64Builder::new(),
            ty: Float64Builder::new(),
            tz: Float64Builder::new(),
            qx: Float64Builder::new(),
            qy: Float64Builder::new(),
            qz: Float64Builder::new(),
            qw: Float64Builder::new(),
            len: 0,
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        t_ns: i64,
        entity: &str,
        tx: f64,
        ty: f64,
        tz: f64,
        qx: f64,
        qy: f64,
        qz: f64,
        qw: f64,
    ) {
        self.t_ns.append_value(t_ns);
        self.entity.append_value(entity);
        self.tx.append_value(tx);
        self.ty.append_value(ty);
        self.tz.append_value(tz);
        self.qx.append_value(qx);
        self.qy.append_value(qy);
        self.qz.append_value(qz);
        self.qw.append_value(qw);
        self.len += 1;
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.t_ns.finish()),
            Arc::new(self.entity.finish()),
            Arc::new(self.tx.finish()),
            Arc::new(self.ty.finish()),
            Arc::new(self.tz.finish()),
            Arc::new(self.qx.finish()),
            Arc::new(self.qy.finish()),
            Arc::new(self.qz.finish()),
            Arc::new(self.qw.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

pub struct ScalarBatcher {
    schema: SchemaRef,
    t_ns: Int64Builder,
    entity: StringBuilder,
    value: Float64Builder,
    unit: StringBuilder,
    len: usize,
}

impl ScalarBatcher {
    fn new() -> Self {
        Self {
            schema: schema_for(ComponentKind::Scalar),
            t_ns: Int64Builder::new(),
            entity: StringBuilder::new(),
            value: Float64Builder::new(),
            unit: StringBuilder::new(),
            len: 0,
        }
    }
    fn append(&mut self, t_ns: i64, entity: &str, value: f64, unit: Option<&str>) {
        self.t_ns.append_value(t_ns);
        self.entity.append_value(entity);
        self.value.append_value(value);
        match unit {
            Some(u) => self.unit.append_value(u),
            None => self.unit.append_null(),
        }
        self.len += 1;
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.t_ns.finish()),
            Arc::new(self.entity.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.unit.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

pub struct ImageRefBatcher {
    schema: SchemaRef,
    t_ns: Int64Builder,
    entity: StringBuilder,
    uri: StringBuilder,
    width: Int32Builder,
    height: Int32Builder,
    format: StringBuilder,
    len: usize,
}

impl ImageRefBatcher {
    fn new() -> Self {
        Self {
            schema: schema_for(ComponentKind::ImageRef),
            t_ns: Int64Builder::new(),
            entity: StringBuilder::new(),
            uri: StringBuilder::new(),
            width: Int32Builder::new(),
            height: Int32Builder::new(),
            format: StringBuilder::new(),
            len: 0,
        }
    }
    fn append(
        &mut self,
        t_ns: i64,
        entity: &str,
        uri: &str,
        width: i32,
        height: i32,
        format: &str,
    ) {
        self.t_ns.append_value(t_ns);
        self.entity.append_value(entity);
        self.uri.append_value(uri);
        self.width.append_value(width);
        self.height.append_value(height);
        self.format.append_value(format);
        self.len += 1;
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.t_ns.finish()),
            Arc::new(self.entity.finish()),
            Arc::new(self.uri.finish()),
            Arc::new(self.width.finish()),
            Arc::new(self.height.finish()),
            Arc::new(self.format.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

pub struct AnnotationBatcher {
    schema: SchemaRef,
    t_ns: Int64Builder,
    entity: StringBuilder,
    label: StringBuilder,
    len: usize,
}

impl AnnotationBatcher {
    fn new() -> Self {
        Self {
            schema: schema_for(ComponentKind::Annotation),
            t_ns: Int64Builder::new(),
            entity: StringBuilder::new(),
            label: StringBuilder::new(),
            len: 0,
        }
    }
    fn append(&mut self, t_ns: i64, entity: &str, label: &str) {
        self.t_ns.append_value(t_ns);
        self.entity.append_value(entity);
        self.label.append_value(label);
        self.len += 1;
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.t_ns.finish()),
            Arc::new(self.entity.finish()),
            Arc::new(self.label.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

pub struct Points3DBatcher {
    schema: SchemaRef,
    t_ns: Int64Builder,
    entity: StringBuilder,
    points: ListBuilder<StructBuilder>,
    len: usize,
}

impl Points3DBatcher {
    fn new() -> Self {
        let point_fields: Fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
        ]);
        let struct_builder = StructBuilder::from_fields(point_fields, 0);
        Self {
            schema: schema_for(ComponentKind::Points3D),
            t_ns: Int64Builder::new(),
            entity: StringBuilder::new(),
            points: ListBuilder::new(struct_builder),
            len: 0,
        }
    }
    fn append(&mut self, t_ns: i64, entity: &str, points: &[[f64; 3]]) {
        self.t_ns.append_value(t_ns);
        self.entity.append_value(entity);
        let inner = self.points.values();
        for p in points {
            inner
                .field_builder::<Float64Builder>(0)
                .expect("x builder")
                .append_value(p[0]);
            inner
                .field_builder::<Float64Builder>(1)
                .expect("y builder")
                .append_value(p[1]);
            inner
                .field_builder::<Float64Builder>(2)
                .expect("z builder")
                .append_value(p[2]);
            inner.append(true);
        }
        self.points.append(true);
        self.len += 1;
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.t_ns.finish()),
            Arc::new(self.entity.finish()),
            Arc::new(self.points.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqd_core::{ComponentName, EntityPath, Event, Payload, Timeline};

    fn tf_event(t_ns: i64, id: u32) -> Event {
        Event {
            entity_path: EntityPath::new(format!("/agent/{id}")).unwrap(),
            timeline: Timeline::sim(),
            t_ns,
            component: ComponentName::new("pose"),
            payload: Payload::Transform3D {
                tx: id as f64,
                ty: 0.0,
                tz: 0.0,
                qx: 0.0,
                qy: 0.0,
                qz: 0.0,
                qw: 1.0,
            },
        }
    }

    #[test]
    fn transform3d_batcher_roundtrip() {
        let mut b = KindBatcher::new(ComponentKind::Transform3D);
        for i in 0..10 {
            b.append(&tf_event(i, i as u32)).unwrap();
        }
        assert_eq!(b.len(), 10);
        let batch = b.drain().unwrap().expect("non-empty");
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn kind_mismatch_is_rejected() {
        let mut b = KindBatcher::new(ComponentKind::Scalar);
        let e = tf_event(0, 0);
        assert!(b.append(&e).is_err());
    }

    #[test]
    fn empty_drain_returns_none() {
        let mut b = KindBatcher::new(ComponentKind::Annotation);
        assert!(b.drain().unwrap().is_none());
    }
}
