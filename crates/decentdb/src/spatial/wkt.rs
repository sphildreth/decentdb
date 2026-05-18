use crate::spatial::types::{
    CoordinateDimensions, Position, SpatialError, SpatialGeometry, SpatialKind, SpatialValue,
};

pub(crate) fn to_wkt(value: &SpatialValue) -> String {
    let dim = dimension_suffix(value.dimensions);
    let kind = kind_name(value.kind());
    let body = match &value.geometry {
        SpatialGeometry::Point(point) => format!("({})", format_position(*point, value.dimensions)),
        SpatialGeometry::LineString(line) => {
            format!("({})", format_positions(line, value.dimensions))
        }
        SpatialGeometry::Polygon(polygon) => {
            let rings = polygon
                .iter()
                .map(|ring| format!("({})", format_positions(ring, value.dimensions)))
                .collect::<Vec<_>>()
                .join(",");
            format!("({rings})")
        }
        SpatialGeometry::MultiPoint(points) => {
            let points = points
                .iter()
                .map(|point| format!("({})", format_position(*point, value.dimensions)))
                .collect::<Vec<_>>()
                .join(",");
            format!("({points})")
        }
        SpatialGeometry::MultiLineString(lines) => {
            let lines = lines
                .iter()
                .map(|line| format!("({})", format_positions(line, value.dimensions)))
                .collect::<Vec<_>>()
                .join(",");
            format!("({lines})")
        }
        SpatialGeometry::MultiPolygon(polygons) => {
            let polygons = polygons
                .iter()
                .map(|polygon| {
                    let rings = polygon
                        .iter()
                        .map(|ring| format!("({})", format_positions(ring, value.dimensions)))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("({rings})")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("({polygons})")
        }
    };

    if dim.is_empty() {
        format!("SRID={};{}{body}", value.srid, kind)
    } else {
        format!("SRID={};{} {}{body}", value.srid, kind, dim)
    }
}

pub(crate) fn from_wkt(input: &str, default_srid: u32) -> Result<SpatialValue, SpatialError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(SpatialError::Parse("empty WKT".to_string()));
    }

    let (srid, wkt_body) = parse_srid_prefix(trimmed, default_srid)?;
    let open_idx = wkt_body
        .find('(')
        .ok_or_else(|| SpatialError::Parse("missing geometry body".to_string()))?;

    let header = wkt_body[..open_idx].trim();
    let body = wkt_body[open_idx..].trim();
    let (kind, explicit_dimensions) = parse_header(header)?;

    let geometry = parse_geometry(kind, body, explicit_dimensions)?;
    let dimensions = infer_dimensions(&geometry, explicit_dimensions)?;
    SpatialValue::new(srid, dimensions, geometry)
}

fn parse_srid_prefix(input: &str, default_srid: u32) -> Result<(u32, &str), SpatialError> {
    let upper = input.to_ascii_uppercase();
    if !upper.starts_with("SRID=") {
        return Ok((default_srid, input));
    }

    let semi_idx = input
        .find(';')
        .ok_or_else(|| SpatialError::Parse("SRID prefix missing ';'".to_string()))?;
    let srid_part = input[5..semi_idx].trim();
    let srid = srid_part
        .parse::<u32>()
        .map_err(|_| SpatialError::Parse("invalid SRID prefix".to_string()))?;
    Ok((srid, input[semi_idx + 1..].trim()))
}

fn parse_header(header: &str) -> Result<(SpatialKind, Option<CoordinateDimensions>), SpatialError> {
    if header.is_empty() {
        return Err(SpatialError::Parse("missing WKT geometry type".to_string()));
    }

    let mut tokens = header.split_whitespace();
    let primary = tokens
        .next()
        .ok_or_else(|| SpatialError::Parse("missing WKT geometry type".to_string()))?
        .to_ascii_uppercase();
    let secondary = tokens.next().map(|t| t.to_ascii_uppercase());

    let (kind_text, dims_from_primary) = split_kind_and_dimensions(&primary)?;
    let kind = parse_kind(&kind_text)?;

    let dims = match secondary {
        Some(dim_text) => Some(parse_dimension_token(&dim_text)?),
        None => dims_from_primary,
    };

    Ok((kind, dims))
}

fn split_kind_and_dimensions(
    token: &str,
) -> Result<(String, Option<CoordinateDimensions>), SpatialError> {
    if let Some(kind) = token.strip_suffix("ZM") {
        return Ok((kind.to_string(), Some(CoordinateDimensions::Xyzm)));
    }
    if let Some(kind) = token.strip_suffix('Z') {
        return Ok((kind.to_string(), Some(CoordinateDimensions::Xyz)));
    }
    if let Some(kind) = token.strip_suffix('M') {
        return Ok((kind.to_string(), Some(CoordinateDimensions::Xym)));
    }
    Ok((token.to_string(), None))
}

fn parse_kind(kind: &str) -> Result<SpatialKind, SpatialError> {
    match kind {
        "POINT" => Ok(SpatialKind::Point),
        "LINESTRING" => Ok(SpatialKind::LineString),
        "POLYGON" => Ok(SpatialKind::Polygon),
        "MULTIPOINT" => Ok(SpatialKind::MultiPoint),
        "MULTILINESTRING" => Ok(SpatialKind::MultiLineString),
        "MULTIPOLYGON" => Ok(SpatialKind::MultiPolygon),
        _ => Err(SpatialError::Unsupported(format!(
            "unsupported WKT geometry type {kind}"
        ))),
    }
}

fn parse_dimension_token(token: &str) -> Result<CoordinateDimensions, SpatialError> {
    match token {
        "Z" => Ok(CoordinateDimensions::Xyz),
        "M" => Ok(CoordinateDimensions::Xym),
        "ZM" => Ok(CoordinateDimensions::Xyzm),
        _ => Err(SpatialError::Parse(format!(
            "unsupported WKT dimension token {token}"
        ))),
    }
}

fn parse_geometry(
    kind: SpatialKind,
    body: &str,
    explicit_dimensions: Option<CoordinateDimensions>,
) -> Result<SpatialGeometry, SpatialError> {
    match kind {
        SpatialKind::Point => {
            let tuple = strip_parens(body)?;
            let point = parse_position(tuple, explicit_dimensions)?;
            Ok(SpatialGeometry::Point(point))
        }
        SpatialKind::LineString => {
            let tuple = strip_parens(body)?;
            let line = parse_position_list(tuple, explicit_dimensions)?;
            Ok(SpatialGeometry::LineString(line))
        }
        SpatialKind::Polygon => {
            let polygon_body = strip_parens(body)?;
            let mut rings = Vec::new();
            for ring_text in split_top_level(polygon_body, ',') {
                let ring = parse_position_list(strip_parens(ring_text)?, explicit_dimensions)?;
                rings.push(ring);
            }
            Ok(SpatialGeometry::Polygon(rings))
        }
        SpatialKind::MultiPoint => {
            let contents = strip_parens(body)?;
            let entries = split_top_level(contents, ',');
            let mut points = Vec::with_capacity(entries.len());
            for entry in entries {
                let piece = entry.trim();
                let coord_text = if piece.starts_with('(') {
                    strip_parens(piece)?
                } else {
                    piece
                };
                points.push(parse_position(coord_text, explicit_dimensions)?);
            }
            Ok(SpatialGeometry::MultiPoint(points))
        }
        SpatialKind::MultiLineString => {
            let contents = strip_parens(body)?;
            let mut lines = Vec::new();
            for line_text in split_top_level(contents, ',') {
                let line = parse_position_list(strip_parens(line_text)?, explicit_dimensions)?;
                lines.push(line);
            }
            Ok(SpatialGeometry::MultiLineString(lines))
        }
        SpatialKind::MultiPolygon => {
            let contents = strip_parens(body)?;
            let mut polygons = Vec::new();
            for polygon_text in split_top_level(contents, ',') {
                let polygon_body = strip_parens(polygon_text)?;
                let mut rings = Vec::new();
                for ring_text in split_top_level(polygon_body, ',') {
                    let ring = parse_position_list(strip_parens(ring_text)?, explicit_dimensions)?;
                    rings.push(ring);
                }
                polygons.push(rings);
            }
            Ok(SpatialGeometry::MultiPolygon(polygons))
        }
    }
}

fn parse_position_list(
    input: &str,
    explicit_dimensions: Option<CoordinateDimensions>,
) -> Result<Vec<Position>, SpatialError> {
    let mut points = Vec::new();
    for part in split_top_level(input, ',') {
        points.push(parse_position(part, explicit_dimensions)?);
    }
    Ok(points)
}

fn parse_position(
    input: &str,
    explicit_dimensions: Option<CoordinateDimensions>,
) -> Result<Position, SpatialError> {
    let ordinates = input
        .split_whitespace()
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| {
            token
                .parse::<f64>()
                .map_err(|_| SpatialError::Parse(format!("invalid numeric ordinate '{token}'")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let dimensions = match explicit_dimensions {
        Some(dim) => dim,
        None => match ordinates.len() {
            2 => CoordinateDimensions::Xy,
            3 => CoordinateDimensions::Xyz,
            4 => CoordinateDimensions::Xyzm,
            n => return Err(SpatialError::Parse(format!("invalid ordinate count {n}"))),
        },
    };

    Position::from_ordinates(dimensions, &ordinates)
}

fn strip_parens(input: &str) -> Result<&str, SpatialError> {
    let trimmed = input.trim();
    if !(trimmed.starts_with('(') && trimmed.ends_with(')')) {
        return Err(SpatialError::Parse(
            "expected parenthesized tuple/list".to_string(),
        ));
    }
    Ok(&trimmed[1..trimmed.len() - 1])
}

fn split_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;

    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }

        if ch == delimiter && depth == 0 {
            result.push(input[start..idx].trim());
            start = idx + delimiter.len_utf8();
        }
    }

    result.push(input[start..].trim());
    result
}

fn infer_dimensions(
    geometry: &SpatialGeometry,
    explicit_dimensions: Option<CoordinateDimensions>,
) -> Result<CoordinateDimensions, SpatialError> {
    if let Some(explicit) = explicit_dimensions {
        return Ok(explicit);
    }

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
                    "mixed coordinate dimensions in WKT geometry".to_string(),
                ));
            }
        } else {
            dimensions = Some(candidate);
        }
    }

    Ok(dimensions.unwrap_or(CoordinateDimensions::Xy))
}

fn format_position(position: Position, dimensions: CoordinateDimensions) -> String {
    position
        .as_ordinates(dimensions)
        .iter()
        .map(|value| trim_float(*value))
        .collect::<Vec<_>>()
        .join(" ")
}

fn format_positions(points: &[Position], dimensions: CoordinateDimensions) -> String {
    points
        .iter()
        .map(|point| format_position(*point, dimensions))
        .collect::<Vec<_>>()
        .join(",")
}

fn trim_float(value: f64) -> String {
    let mut s = format!("{value:.15}");
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn kind_name(kind: SpatialKind) -> &'static str {
    match kind {
        SpatialKind::Point => "POINT",
        SpatialKind::LineString => "LINESTRING",
        SpatialKind::Polygon => "POLYGON",
        SpatialKind::MultiPoint => "MULTIPOINT",
        SpatialKind::MultiLineString => "MULTILINESTRING",
        SpatialKind::MultiPolygon => "MULTIPOLYGON",
    }
}

fn dimension_suffix(dimensions: CoordinateDimensions) -> &'static str {
    match dimensions {
        CoordinateDimensions::Xy => "",
        CoordinateDimensions::Xyz => "Z",
        CoordinateDimensions::Xym => "M",
        CoordinateDimensions::Xyzm => "ZM",
    }
}

#[cfg(test)]
mod tests {
    use crate::spatial::types::{CoordinateDimensions, SpatialGeometry, SpatialValue};
    use crate::spatial::wkt::{from_wkt, to_wkt};

    #[test]
    fn parses_and_serializes_point_zm() {
        let parsed = from_wkt("SRID=4326;POINT ZM (1 2 3 4)", 0).expect("point zm should parse");
        assert_eq!(parsed.srid, 4326);
        assert_eq!(parsed.dimensions, CoordinateDimensions::Xyzm);

        let rendered = to_wkt(&parsed);
        assert_eq!(rendered, "SRID=4326;POINT ZM(1 2 3 4)");
    }

    #[test]
    fn parses_multi_polygon() {
        let parsed = from_wkt(
            "SRID=3857;MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))",
            0,
        )
        .expect("multi polygon should parse");

        let SpatialGeometry::MultiPolygon(polygons) = parsed.geometry else {
            panic!("expected multipolygon")
        };
        assert_eq!(polygons.len(), 2);
    }

    #[test]
    fn roundtrip_linestring_xy() {
        let parsed = from_wkt("LINESTRING(0 0,1 1,2 1)", 3857).expect("linestring parse");
        let wkt = to_wkt(&parsed);
        let reparsed = from_wkt(&wkt, 0).expect("roundtrip parse");

        let expected = SpatialValue::new(parsed.srid, parsed.dimensions, parsed.geometry.clone())
            .expect("valid value");
        assert_eq!(reparsed, expected);
    }
}
