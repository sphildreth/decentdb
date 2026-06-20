use crate::spatial::types::{
    CoordinateDimensions, LineString, Polygon, Position, SpatialError, SpatialGeometry,
    SpatialKind, SpatialValue,
};

const FLAG_Z: u32 = 0x8000_0000;
const FLAG_M: u32 = 0x4000_0000;
const FLAG_SRID: u32 = 0x2000_0000;
const TYPE_MASK: u32 = 0x0000_00ff;
const LITTLE_ENDIAN_MARKER: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endian {
    Little,
    Big,
}

impl Endian {
    fn from_marker(marker: u8) -> Result<Self, SpatialError> {
        match marker {
            0 => Ok(Self::Big),
            1 => Ok(Self::Little),
            _ => Err(SpatialError::Parse("invalid endian marker".to_string())),
        }
    }
}

pub(crate) fn normalize_ewkb(bytes: &[u8]) -> Result<Vec<u8>, SpatialError> {
    let value = from_ewkb(bytes)?;
    Ok(to_ewkb(&value))
}

pub(crate) fn normalize_wkb_with_default_srid(
    bytes: &[u8],
    default_srid: u32,
) -> Result<Vec<u8>, SpatialError> {
    let value = from_wkb_with_default_srid(bytes, default_srid)?;
    Ok(to_ewkb(&value))
}

pub(crate) fn to_ewkb(value: &SpatialValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_geometry(
        &value.geometry,
        value.kind(),
        value.dimensions,
        Some(value.srid),
        &mut out,
    );
    out
}

pub(crate) fn from_ewkb(bytes: &[u8]) -> Result<SpatialValue, SpatialError> {
    from_wkb(bytes, None)
}

pub(crate) fn from_wkb_with_default_srid(
    bytes: &[u8],
    default_srid: u32,
) -> Result<SpatialValue, SpatialError> {
    from_wkb(bytes, Some(default_srid))
}

fn from_wkb(bytes: &[u8], default_srid: Option<u32>) -> Result<SpatialValue, SpatialError> {
    let mut reader = ByteReader::new(bytes);
    let parsed = read_geometry(&mut reader, default_srid, None, None)?;

    if reader.remaining() != 0 {
        return Err(SpatialError::Parse(
            "trailing bytes after EWKB geometry".to_string(),
        ));
    }

    let srid = parsed
        .srid
        .ok_or_else(|| SpatialError::Parse("normalized EWKB requires SRID".to_string()))?;

    SpatialValue::new(srid, parsed.dimensions, parsed.geometry)
}

#[derive(Debug)]
struct ParsedGeometry {
    kind: SpatialKind,
    dimensions: CoordinateDimensions,
    srid: Option<u32>,
    geometry: SpatialGeometry,
}

fn write_geometry(
    geometry: &SpatialGeometry,
    kind: SpatialKind,
    dimensions: CoordinateDimensions,
    srid: Option<u32>,
    out: &mut Vec<u8>,
) {
    out.push(LITTLE_ENDIAN_MARKER);

    let mut type_code = kind.as_code();
    if dimensions.has_z() {
        type_code |= FLAG_Z;
    }
    if dimensions.has_m() {
        type_code |= FLAG_M;
    }
    if srid.is_some() {
        type_code |= FLAG_SRID;
    }
    out.extend_from_slice(&type_code.to_le_bytes());

    if let Some(srid) = srid {
        out.extend_from_slice(&srid.to_le_bytes());
    }

    write_geometry_body(geometry, dimensions, out);
}

fn write_geometry_body(
    geometry: &SpatialGeometry,
    dimensions: CoordinateDimensions,
    out: &mut Vec<u8>,
) {
    match geometry {
        SpatialGeometry::Point(point) => write_position(*point, dimensions, out),
        SpatialGeometry::LineString(line) => write_line(line, dimensions, out),
        SpatialGeometry::Polygon(polygon) => write_polygon(polygon, dimensions, out),
        SpatialGeometry::MultiPoint(points) => {
            out.extend_from_slice(&(points.len() as u32).to_le_bytes());
            for point in points {
                write_geometry(
                    &SpatialGeometry::Point(*point),
                    SpatialKind::Point,
                    dimensions,
                    None,
                    out,
                );
            }
        }
        SpatialGeometry::MultiLineString(lines) => {
            out.extend_from_slice(&(lines.len() as u32).to_le_bytes());
            for line in lines {
                write_geometry(
                    &SpatialGeometry::LineString(line.clone()),
                    SpatialKind::LineString,
                    dimensions,
                    None,
                    out,
                );
            }
        }
        SpatialGeometry::MultiPolygon(polygons) => {
            out.extend_from_slice(&(polygons.len() as u32).to_le_bytes());
            for polygon in polygons {
                write_geometry(
                    &SpatialGeometry::Polygon(polygon.clone()),
                    SpatialKind::Polygon,
                    dimensions,
                    None,
                    out,
                );
            }
        }
    }
}

fn write_line(line: &LineString, dimensions: CoordinateDimensions, out: &mut Vec<u8>) {
    out.extend_from_slice(&(line.len() as u32).to_le_bytes());
    for &point in line {
        write_position(point, dimensions, out);
    }
}

fn write_polygon(polygon: &Polygon, dimensions: CoordinateDimensions, out: &mut Vec<u8>) {
    out.extend_from_slice(&(polygon.len() as u32).to_le_bytes());
    for ring in polygon {
        out.extend_from_slice(&(ring.len() as u32).to_le_bytes());
        for &point in ring {
            write_position(point, dimensions, out);
        }
    }
}

fn write_position(point: Position, dimensions: CoordinateDimensions, out: &mut Vec<u8>) {
    for ordinate in point.as_ordinates(dimensions) {
        out.extend_from_slice(&ordinate.to_le_bytes());
    }
}

fn read_geometry(
    reader: &mut ByteReader<'_>,
    inherited_srid: Option<u32>,
    expected_kind: Option<SpatialKind>,
    expected_dimensions: Option<CoordinateDimensions>,
) -> Result<ParsedGeometry, SpatialError> {
    let endian = Endian::from_marker(reader.read_u8()?)?;
    let mut type_code = reader.read_u32(endian)?;

    let has_z = (type_code & FLAG_Z) != 0;
    let has_m = (type_code & FLAG_M) != 0;
    let has_srid = (type_code & FLAG_SRID) != 0;
    type_code &= TYPE_MASK;

    let kind = SpatialKind::from_code(type_code).ok_or_else(|| {
        SpatialError::Unsupported(format!("unsupported EWKB type code {type_code}"))
    })?;
    if let Some(expected) = expected_kind {
        if kind != expected {
            return Err(SpatialError::Parse(format!(
                "expected nested {:?}, got {:?}",
                expected, kind
            )));
        }
    }

    let dimensions = CoordinateDimensions::from_flags(has_z, has_m);
    if let Some(expected) = expected_dimensions {
        if expected != dimensions {
            return Err(SpatialError::Parse(
                "mixed dimensions in nested geometry are not supported".to_string(),
            ));
        }
    }

    let srid = if has_srid {
        Some(reader.read_u32(endian)?)
    } else {
        inherited_srid
    };

    let geometry = read_geometry_body(reader, endian, kind, dimensions, srid)?;

    Ok(ParsedGeometry {
        kind,
        dimensions,
        srid,
        geometry,
    })
}

fn read_geometry_body(
    reader: &mut ByteReader<'_>,
    endian: Endian,
    kind: SpatialKind,
    dimensions: CoordinateDimensions,
    srid: Option<u32>,
) -> Result<SpatialGeometry, SpatialError> {
    match kind {
        SpatialKind::Point => Ok(SpatialGeometry::Point(read_position(
            reader, endian, dimensions,
        )?)),
        SpatialKind::LineString => {
            let point_count = reader.read_u32(endian)? as usize;
            let mut line = Vec::with_capacity(point_count);
            for _ in 0..point_count {
                line.push(read_position(reader, endian, dimensions)?);
            }
            Ok(SpatialGeometry::LineString(line))
        }
        SpatialKind::Polygon => {
            let ring_count = reader.read_u32(endian)? as usize;
            let mut polygon = Vec::with_capacity(ring_count);
            for _ in 0..ring_count {
                let point_count = reader.read_u32(endian)? as usize;
                let mut ring = Vec::with_capacity(point_count);
                for _ in 0..point_count {
                    ring.push(read_position(reader, endian, dimensions)?);
                }
                polygon.push(ring);
            }
            Ok(SpatialGeometry::Polygon(polygon))
        }
        SpatialKind::MultiPoint => {
            let count = reader.read_u32(endian)? as usize;
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                let parsed =
                    read_geometry(reader, srid, Some(SpatialKind::Point), Some(dimensions))?;
                if parsed.srid != srid {
                    return Err(SpatialError::Parse(
                        "nested SRID must match parent SRID".to_string(),
                    ));
                }
                let SpatialGeometry::Point(point) = parsed.geometry else {
                    return Err(SpatialError::Parse("expected nested point".to_string()));
                };
                points.push(point);
            }
            Ok(SpatialGeometry::MultiPoint(points))
        }
        SpatialKind::MultiLineString => {
            let count = reader.read_u32(endian)? as usize;
            let mut lines = Vec::with_capacity(count);
            for _ in 0..count {
                let parsed = read_geometry(
                    reader,
                    srid,
                    Some(SpatialKind::LineString),
                    Some(dimensions),
                )?;
                if parsed.srid != srid {
                    return Err(SpatialError::Parse(
                        "nested SRID must match parent SRID".to_string(),
                    ));
                }
                let SpatialGeometry::LineString(line) = parsed.geometry else {
                    return Err(SpatialError::Parse(
                        "expected nested linestring".to_string(),
                    ));
                };
                lines.push(line);
            }
            Ok(SpatialGeometry::MultiLineString(lines))
        }
        SpatialKind::MultiPolygon => {
            let count = reader.read_u32(endian)? as usize;
            let mut polygons = Vec::with_capacity(count);
            for _ in 0..count {
                let parsed =
                    read_geometry(reader, srid, Some(SpatialKind::Polygon), Some(dimensions))?;
                if parsed.srid != srid {
                    return Err(SpatialError::Parse(
                        "nested SRID must match parent SRID".to_string(),
                    ));
                }
                let SpatialGeometry::Polygon(polygon) = parsed.geometry else {
                    return Err(SpatialError::Parse("expected nested polygon".to_string()));
                };
                polygons.push(polygon);
            }
            Ok(SpatialGeometry::MultiPolygon(polygons))
        }
    }
}

fn read_position(
    reader: &mut ByteReader<'_>,
    endian: Endian,
    dimensions: CoordinateDimensions,
) -> Result<Position, SpatialError> {
    let mut ordinates = Vec::with_capacity(dimensions.ordinate_count());
    for _ in 0..dimensions.ordinate_count() {
        ordinates.push(reader.read_f64(endian)?);
    }
    Position::from_ordinates(dimensions, &ordinates)
}

struct ByteReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], SpatialError> {
        let end = self.offset.saturating_add(len);
        if end > self.bytes.len() {
            return Err(SpatialError::Truncated);
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, SpatialError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self, endian: Endian) -> Result<u32, SpatialError> {
        let bytes = self.read_exact(4)?;
        let mut arr = [0u8; 4];
        arr.copy_from_slice(bytes);
        Ok(match endian {
            Endian::Little => u32::from_le_bytes(arr),
            Endian::Big => u32::from_be_bytes(arr),
        })
    }

    fn read_f64(&mut self, endian: Endian) -> Result<f64, SpatialError> {
        let bytes = self.read_exact(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Ok(match endian {
            Endian::Little => f64::from_le_bytes(arr),
            Endian::Big => f64::from_be_bytes(arr),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::spatial::ewkb::{from_ewkb, normalize_ewkb, to_ewkb};
    use crate::spatial::types::{
        CoordinateDimensions, Position, SpatialError, SpatialGeometry, SpatialValue,
    };

    #[test]
    fn roundtrip_point_xyzm() {
        let value = SpatialValue::new(
            4326,
            CoordinateDimensions::Xyzm,
            SpatialGeometry::Point(
                Position::for_dimensions(
                    1.0,
                    2.0,
                    Some(3.0),
                    Some(4.0),
                    CoordinateDimensions::Xyzm,
                )
                .expect("xyzm point"),
            ),
        )
        .expect("valid spatial value");

        let bytes = to_ewkb(&value);
        let parsed = from_ewkb(&bytes).expect("ewkb should parse");
        assert_eq!(parsed, value);
    }

    #[test]
    fn roundtrip_multi_polygon_xy() {
        let polygon_a = vec![vec![
            Position::xy(0.0, 0.0),
            Position::xy(4.0, 0.0),
            Position::xy(4.0, 4.0),
            Position::xy(0.0, 0.0),
        ]];
        let polygon_b = vec![vec![
            Position::xy(10.0, 10.0),
            Position::xy(11.0, 10.0),
            Position::xy(11.0, 11.0),
            Position::xy(10.0, 10.0),
        ]];

        let value = SpatialValue::new(
            3857,
            CoordinateDimensions::Xy,
            SpatialGeometry::MultiPolygon(vec![polygon_a, polygon_b]),
        )
        .expect("valid multi polygon");

        let bytes = to_ewkb(&value);
        let parsed = from_ewkb(&bytes).expect("ewkb should parse");
        assert_eq!(parsed, value);
    }

    #[test]
    fn normalize_big_endian_payload() {
        // Big-endian point with SRID 4326, coordinates (1, 2).
        let mut bytes = vec![0u8];
        bytes.extend_from_slice(&0x2000_0001u32.to_be_bytes());
        bytes.extend_from_slice(&4326u32.to_be_bytes());
        bytes.extend_from_slice(&1f64.to_be_bytes());
        bytes.extend_from_slice(&2f64.to_be_bytes());

        let normalized = normalize_ewkb(&bytes).expect("must normalize");
        let parsed = from_ewkb(&normalized).expect("normalized should parse");
        assert_eq!(parsed.srid, 4326);
    }

    #[test]
    fn reject_unknown_type_code() {
        let mut bytes = vec![1u8];
        bytes.extend_from_slice(&0x2000_00FFu32.to_le_bytes());
        bytes.extend_from_slice(&4326u32.to_le_bytes());

        let err = from_ewkb(&bytes).unwrap_err();
        let msg = err.to_string().to_lowercase();
        assert!(msg.contains("unsupported"));
    }

    #[test]
    fn reject_truncated_payloads() {
        let err = from_ewkb(&[1u8, 1, 0]).unwrap_err();
        assert_eq!(err, SpatialError::Truncated);
    }

    #[test]
    fn reject_trailing_bytes() {
        let value = SpatialValue::new(
            4326,
            CoordinateDimensions::Xy,
            SpatialGeometry::Point(Position::xy(1.0, 2.0)),
        )
        .expect("valid spatial value");

        let mut bytes = to_ewkb(&value);
        bytes.push(0xAA);

        let err = from_ewkb(&bytes).unwrap_err().to_string().to_lowercase();
        assert!(err.contains("trailing bytes"));
    }
}
