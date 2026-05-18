use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub(crate) enum SpatialError {
    #[error("invalid spatial input: {0}")]
    InvalidInput(String),
    #[error("unsupported spatial operation: {0}")]
    Unsupported(String),
    #[error("spatial parse error: {0}")]
    Parse(String),
    #[error("truncated or invalid binary payload")]
    Truncated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoordinateDimensions {
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

impl CoordinateDimensions {
    pub(crate) fn has_z(self) -> bool {
        matches!(self, Self::Xyz | Self::Xyzm)
    }

    pub(crate) fn has_m(self) -> bool {
        matches!(self, Self::Xym | Self::Xyzm)
    }

    pub(crate) fn ordinate_count(self) -> usize {
        match self {
            Self::Xy => 2,
            Self::Xyz | Self::Xym => 3,
            Self::Xyzm => 4,
        }
    }

    pub(crate) fn from_flags(has_z: bool, has_m: bool) -> Self {
        match (has_z, has_m) {
            (false, false) => Self::Xy,
            (true, false) => Self::Xyz,
            (false, true) => Self::Xym,
            (true, true) => Self::Xyzm,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum SpatialKind {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
}

impl SpatialKind {
    pub(crate) fn from_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(Self::Point),
            2 => Some(Self::LineString),
            3 => Some(Self::Polygon),
            4 => Some(Self::MultiPoint),
            5 => Some(Self::MultiLineString),
            6 => Some(Self::MultiPolygon),
            _ => None,
        }
    }

    pub(crate) fn as_code(self) -> u32 {
        self as u32
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Position {
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) z: Option<f64>,
    pub(crate) m: Option<f64>,
}

impl Position {
    pub(crate) fn xy(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            z: None,
            m: None,
        }
    }

    pub(crate) fn with_optional(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Self {
        Self { x, y, z, m }
    }

    pub(crate) fn for_dimensions(
        x: f64,
        y: f64,
        z: Option<f64>,
        m: Option<f64>,
        dimensions: CoordinateDimensions,
    ) -> Result<Self, SpatialError> {
        let pos = Self { x, y, z, m };
        pos.validate_for_dimensions(dimensions)?;
        Ok(pos)
    }

    pub(crate) fn from_ordinates(
        dimensions: CoordinateDimensions,
        ordinates: &[f64],
    ) -> Result<Self, SpatialError> {
        if ordinates.len() != dimensions.ordinate_count() {
            return Err(SpatialError::InvalidInput(format!(
                "expected {} ordinates, got {}",
                dimensions.ordinate_count(),
                ordinates.len()
            )));
        }
        let x = ordinates[0];
        let y = ordinates[1];
        let (z, m) = match dimensions {
            CoordinateDimensions::Xy => (None, None),
            CoordinateDimensions::Xyz => (Some(ordinates[2]), None),
            CoordinateDimensions::Xym => (None, Some(ordinates[2])),
            CoordinateDimensions::Xyzm => (Some(ordinates[2]), Some(ordinates[3])),
        };
        Ok(Self { x, y, z, m })
    }

    pub(crate) fn as_ordinates(self, dimensions: CoordinateDimensions) -> Vec<f64> {
        match dimensions {
            CoordinateDimensions::Xy => vec![self.x, self.y],
            CoordinateDimensions::Xyz => vec![self.x, self.y, self.z.unwrap_or_default()],
            CoordinateDimensions::Xym => vec![self.x, self.y, self.m.unwrap_or_default()],
            CoordinateDimensions::Xyzm => vec![
                self.x,
                self.y,
                self.z.unwrap_or_default(),
                self.m.unwrap_or_default(),
            ],
        }
    }

    pub(crate) fn validate_for_dimensions(
        self,
        dimensions: CoordinateDimensions,
    ) -> Result<(), SpatialError> {
        match dimensions {
            CoordinateDimensions::Xy => {
                if self.z.is_some() || self.m.is_some() {
                    return Err(SpatialError::InvalidInput(
                        "XY position cannot contain Z or M ordinates".to_string(),
                    ));
                }
            }
            CoordinateDimensions::Xyz => {
                if self.z.is_none() || self.m.is_some() {
                    return Err(SpatialError::InvalidInput(
                        "XYZ position requires Z and forbids M".to_string(),
                    ));
                }
            }
            CoordinateDimensions::Xym => {
                if self.z.is_some() || self.m.is_none() {
                    return Err(SpatialError::InvalidInput(
                        "XYM position requires M and forbids Z".to_string(),
                    ));
                }
            }
            CoordinateDimensions::Xyzm => {
                if self.z.is_none() || self.m.is_none() {
                    return Err(SpatialError::InvalidInput(
                        "XYZM position requires both Z and M".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn xy_equals(self, other: Self) -> bool {
        self.x == other.x && self.y == other.y
    }
}

pub(crate) type LineString = Vec<Position>;
pub(crate) type LinearRing = Vec<Position>;
pub(crate) type Polygon = Vec<LinearRing>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SpatialGeometry {
    Point(Position),
    LineString(LineString),
    Polygon(Polygon),
    MultiPoint(Vec<Position>),
    MultiLineString(Vec<LineString>),
    MultiPolygon(Vec<Polygon>),
}

impl SpatialGeometry {
    pub(crate) fn kind(&self) -> SpatialKind {
        match self {
            Self::Point(_) => SpatialKind::Point,
            Self::LineString(_) => SpatialKind::LineString,
            Self::Polygon(_) => SpatialKind::Polygon,
            Self::MultiPoint(_) => SpatialKind::MultiPoint,
            Self::MultiLineString(_) => SpatialKind::MultiLineString,
            Self::MultiPolygon(_) => SpatialKind::MultiPolygon,
        }
    }

    pub(crate) fn validate_for_dimensions(
        &self,
        dimensions: CoordinateDimensions,
    ) -> Result<(), SpatialError> {
        fn validate_positions(
            positions: &[Position],
            dimensions: CoordinateDimensions,
        ) -> Result<(), SpatialError> {
            for &position in positions {
                position.validate_for_dimensions(dimensions)?;
            }
            Ok(())
        }

        match self {
            Self::Point(position) => position.validate_for_dimensions(dimensions),
            Self::LineString(points) => validate_positions(points, dimensions),
            Self::Polygon(rings) => {
                for ring in rings {
                    validate_positions(ring, dimensions)?;
                }
                Ok(())
            }
            Self::MultiPoint(points) => validate_positions(points, dimensions),
            Self::MultiLineString(lines) => {
                for line in lines {
                    validate_positions(line, dimensions)?;
                }
                Ok(())
            }
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        validate_positions(ring, dimensions)?;
                    }
                }
                Ok(())
            }
        }
    }

    pub(crate) fn all_positions<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Position> + 'a> {
        match self {
            Self::Point(position) => Box::new(std::iter::once(position)),
            Self::LineString(points) => Box::new(points.iter()),
            Self::Polygon(rings) => Box::new(rings.iter().flatten()),
            Self::MultiPoint(points) => Box::new(points.iter()),
            Self::MultiLineString(lines) => Box::new(lines.iter().flatten()),
            Self::MultiPolygon(polygons) => Box::new(polygons.iter().flatten().flatten()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpatialValue {
    pub(crate) srid: u32,
    pub(crate) dimensions: CoordinateDimensions,
    pub(crate) geometry: SpatialGeometry,
}

impl SpatialValue {
    pub(crate) fn new(
        srid: u32,
        dimensions: CoordinateDimensions,
        geometry: SpatialGeometry,
    ) -> Result<Self, SpatialError> {
        geometry.validate_for_dimensions(dimensions)?;
        Ok(Self {
            srid,
            dimensions,
            geometry,
        })
    }

    pub(crate) fn kind(&self) -> SpatialKind {
        self.geometry.kind()
    }
}

#[cfg(test)]
mod tests {
    use super::{CoordinateDimensions, Position};

    #[test]
    fn position_roundtrip_ordinates() {
        let position = Position::from_ordinates(CoordinateDimensions::Xyzm, &[1.0, 2.0, 3.0, 4.0])
            .expect("xyzm position should parse");
        assert_eq!(
            position.as_ordinates(CoordinateDimensions::Xyzm),
            vec![1.0, 2.0, 3.0, 4.0]
        );
    }
}
