use crate::spatial::types::{
    CoordinateDimensions, Position, SpatialError, SpatialGeometry, SpatialKind, SpatialValue,
};
use serde_json::{json, Value as JsonValue};

pub(crate) fn from_geojson(input: &str, default_srid: u32) -> Result<SpatialValue, SpatialError> {
    let json: JsonValue =
        serde_json::from_str(input).map_err(|e| SpatialError::Parse(e.to_string()))?;

    let kind_text = json
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| SpatialError::Parse("GeoJSON missing 'type'".to_string()))?;
    let coords = json
        .get("coordinates")
        .ok_or_else(|| SpatialError::Parse("GeoJSON missing 'coordinates'".to_string()))?;

    let kind = parse_kind(kind_text)?;
    let geometry = parse_geometry(kind, coords)?;
    let dimensions = infer_dimensions(&geometry)?;

    SpatialValue::new(default_srid, dimensions, geometry)
}

pub(crate) fn to_geojson(value: &SpatialValue) -> Result<String, SpatialError> {
    let coordinates = geometry_to_coordinates(&value.geometry, value.dimensions);
    let doc = json!({
        "type": kind_name(value.kind()),
        "coordinates": coordinates,
    });

    serde_json::to_string(&doc).map_err(|e| SpatialError::Parse(e.to_string()))
}

fn parse_kind(kind: &str) -> Result<SpatialKind, SpatialError> {
    match kind {
        "Point" => Ok(SpatialKind::Point),
        "LineString" => Ok(SpatialKind::LineString),
        "Polygon" => Ok(SpatialKind::Polygon),
        "MultiPoint" => Ok(SpatialKind::MultiPoint),
        "MultiLineString" => Ok(SpatialKind::MultiLineString),
        "MultiPolygon" => Ok(SpatialKind::MultiPolygon),
        _ => Err(SpatialError::Unsupported(format!(
            "unsupported GeoJSON geometry type {kind}"
        ))),
    }
}

fn parse_geometry(
    kind: SpatialKind,
    coordinates: &JsonValue,
) -> Result<SpatialGeometry, SpatialError> {
    match kind {
        SpatialKind::Point => Ok(SpatialGeometry::Point(parse_position(coordinates)?)),
        SpatialKind::LineString => Ok(SpatialGeometry::LineString(parse_position_array(
            coordinates,
        )?)),
        SpatialKind::Polygon => {
            let rings = parse_position_array_array(coordinates)?;
            Ok(SpatialGeometry::Polygon(rings))
        }
        SpatialKind::MultiPoint => Ok(SpatialGeometry::MultiPoint(parse_position_array(
            coordinates,
        )?)),
        SpatialKind::MultiLineString => {
            let lines = parse_position_array_array(coordinates)?;
            Ok(SpatialGeometry::MultiLineString(lines))
        }
        SpatialKind::MultiPolygon => {
            let polygons = parse_position_array_array_array(coordinates)?;
            Ok(SpatialGeometry::MultiPolygon(polygons))
        }
    }
}

fn parse_position(value: &JsonValue) -> Result<Position, SpatialError> {
    let ordinates = value
        .as_array()
        .ok_or_else(|| SpatialError::Parse("position must be an array".to_string()))?
        .iter()
        .map(|v| {
            v.as_f64()
                .ok_or_else(|| SpatialError::Parse("coordinate must be numeric".to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let dimensions = match ordinates.len() {
        2 => CoordinateDimensions::Xy,
        3 => CoordinateDimensions::Xyz,
        4 => CoordinateDimensions::Xyzm,
        n => {
            return Err(SpatialError::Parse(format!(
                "unsupported coordinate length {n}"
            )))
        }
    };

    Position::from_ordinates(dimensions, &ordinates)
}

fn parse_position_array(value: &JsonValue) -> Result<Vec<Position>, SpatialError> {
    let points = value
        .as_array()
        .ok_or_else(|| SpatialError::Parse("coordinates must be an array".to_string()))?;

    let mut out = Vec::with_capacity(points.len());
    for point in points {
        out.push(parse_position(point)?);
    }
    Ok(out)
}

fn parse_position_array_array(value: &JsonValue) -> Result<Vec<Vec<Position>>, SpatialError> {
    let arrays = value
        .as_array()
        .ok_or_else(|| SpatialError::Parse("coordinates must be an array".to_string()))?;

    let mut out = Vec::with_capacity(arrays.len());
    for array in arrays {
        out.push(parse_position_array(array)?);
    }
    Ok(out)
}

fn parse_position_array_array_array(
    value: &JsonValue,
) -> Result<Vec<Vec<Vec<Position>>>, SpatialError> {
    let arrays = value
        .as_array()
        .ok_or_else(|| SpatialError::Parse("coordinates must be an array".to_string()))?;

    let mut out = Vec::with_capacity(arrays.len());
    for array in arrays {
        out.push(parse_position_array_array(array)?);
    }
    Ok(out)
}

fn infer_dimensions(geometry: &SpatialGeometry) -> Result<CoordinateDimensions, SpatialError> {
    let mut dimensions = None;
    for position in geometry.all_positions() {
        let candidate = match (position.z.is_some(), position.m.is_some()) {
            (false, false) => CoordinateDimensions::Xy,
            (true, false) => CoordinateDimensions::Xyz,
            (false, true) => CoordinateDimensions::Xym,
            (true, true) => CoordinateDimensions::Xyzm,
        };

        if let Some(current) = dimensions {
            if current != candidate {
                return Err(SpatialError::Parse(
                    "mixed coordinate dimensions in GeoJSON geometry".to_string(),
                ));
            }
        } else {
            dimensions = Some(candidate);
        }
    }

    Ok(dimensions.unwrap_or(CoordinateDimensions::Xy))
}

fn geometry_to_coordinates(geometry: &SpatialGeometry, dims: CoordinateDimensions) -> JsonValue {
    match geometry {
        SpatialGeometry::Point(point) => position_to_json(*point, dims),
        SpatialGeometry::LineString(line) => {
            JsonValue::Array(line.iter().map(|p| position_to_json(*p, dims)).collect())
        }
        SpatialGeometry::Polygon(polygon) => JsonValue::Array(
            polygon
                .iter()
                .map(|ring| {
                    JsonValue::Array(ring.iter().map(|p| position_to_json(*p, dims)).collect())
                })
                .collect(),
        ),
        SpatialGeometry::MultiPoint(points) => {
            JsonValue::Array(points.iter().map(|p| position_to_json(*p, dims)).collect())
        }
        SpatialGeometry::MultiLineString(lines) => JsonValue::Array(
            lines
                .iter()
                .map(|line| {
                    JsonValue::Array(line.iter().map(|p| position_to_json(*p, dims)).collect())
                })
                .collect(),
        ),
        SpatialGeometry::MultiPolygon(polygons) => JsonValue::Array(
            polygons
                .iter()
                .map(|polygon| {
                    JsonValue::Array(
                        polygon
                            .iter()
                            .map(|ring| {
                                JsonValue::Array(
                                    ring.iter().map(|p| position_to_json(*p, dims)).collect(),
                                )
                            })
                            .collect(),
                    )
                })
                .collect(),
        ),
    }
}

fn position_to_json(position: Position, dims: CoordinateDimensions) -> JsonValue {
    JsonValue::Array(
        position
            .as_ordinates(dims)
            .into_iter()
            .map(JsonValue::from)
            .collect(),
    )
}

fn kind_name(kind: SpatialKind) -> &'static str {
    match kind {
        SpatialKind::Point => "Point",
        SpatialKind::LineString => "LineString",
        SpatialKind::Polygon => "Polygon",
        SpatialKind::MultiPoint => "MultiPoint",
        SpatialKind::MultiLineString => "MultiLineString",
        SpatialKind::MultiPolygon => "MultiPolygon",
    }
}

#[cfg(test)]
mod tests {
    use crate::spatial::geojson::{from_geojson, to_geojson};
    use crate::spatial::types::{CoordinateDimensions, SpatialGeometry};

    #[test]
    fn parses_point_geojson() {
        let value = from_geojson(r#"{"type":"Point","coordinates":[1,2,3]}"#, 4326)
            .expect("point should parse");
        assert_eq!(value.srid, 4326);
        assert_eq!(value.dimensions, CoordinateDimensions::Xyz);
    }

    #[test]
    fn roundtrip_polygon_geojson() {
        let value = from_geojson(
            r#"{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,0]]]}"#,
            3857,
        )
        .expect("polygon should parse");

        let json = to_geojson(&value).expect("serialization should succeed");
        let reparsed = from_geojson(&json, 3857).expect("roundtrip parse");
        assert_eq!(reparsed.dimensions, CoordinateDimensions::Xy);

        let SpatialGeometry::Polygon(rings) = reparsed.geometry else {
            panic!("expected polygon");
        };
        assert_eq!(rings.len(), 1);
    }
}
