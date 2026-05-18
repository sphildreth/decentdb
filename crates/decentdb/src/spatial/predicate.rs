use crate::spatial::distance::planar_geometry_distance;
use crate::spatial::types::{Position, SpatialError, SpatialGeometry};

const EPS: f64 = 1e-9;

pub(crate) fn intersects(a: &SpatialGeometry, b: &SpatialGeometry) -> Result<bool, SpatialError> {
    Ok(planar_geometry_distance(a, b)? <= EPS)
}

pub(crate) fn contains(a: &SpatialGeometry, b: &SpatialGeometry) -> Result<bool, SpatialError> {
    match (a, b) {
        (SpatialGeometry::Point(pa), SpatialGeometry::Point(pb)) => Ok(pa.xy_equals(*pb)),
        (SpatialGeometry::LineString(line), SpatialGeometry::Point(point)) => {
            Ok(point_on_linestring(*point, line))
        }
        (SpatialGeometry::Polygon(polygon), SpatialGeometry::Point(point)) => {
            Ok(point_in_polygon(*point, polygon))
        }
        (SpatialGeometry::Polygon(polygon), SpatialGeometry::LineString(line)) => {
            contains_linestring(polygon, line)
        }
        (SpatialGeometry::Polygon(a_polygon), SpatialGeometry::Polygon(b_polygon)) => {
            contains_polygon(a_polygon, b_polygon)
        }
        (SpatialGeometry::MultiPoint(points), geom) => contains_multipoint(points, geom),
        (SpatialGeometry::MultiLineString(lines), geom) => contains_multilinestring(lines, geom),
        (SpatialGeometry::MultiPolygon(polygons), geom) => contains_multipolygon(polygons, geom),
        _ => Ok(false),
    }
}

pub(crate) fn within(a: &SpatialGeometry, b: &SpatialGeometry) -> Result<bool, SpatialError> {
    contains(b, a)
}

pub(crate) fn equals(a: &SpatialGeometry, b: &SpatialGeometry) -> Result<bool, SpatialError> {
    match (a, b) {
        (SpatialGeometry::Point(pa), SpatialGeometry::Point(pb)) => Ok(pa.xy_equals(*pb)),
        (SpatialGeometry::LineString(a_line), SpatialGeometry::LineString(b_line)) => {
            Ok(line_equals(a_line, b_line))
        }
        (SpatialGeometry::Polygon(a_polygon), SpatialGeometry::Polygon(b_polygon)) => {
            Ok(polygon_equals(a_polygon, b_polygon))
        }
        (SpatialGeometry::MultiPoint(a_points), SpatialGeometry::MultiPoint(b_points)) => {
            multipoint_equals(a_points, b_points)
        }
        (SpatialGeometry::MultiLineString(a_lines), SpatialGeometry::MultiLineString(b_lines)) => {
            multiline_equals(a_lines, b_lines)
        }
        (SpatialGeometry::MultiPolygon(a_polygons), SpatialGeometry::MultiPolygon(b_polygons)) => {
            multipolygon_equals(a_polygons, b_polygons)
        }
        _ => Ok(false),
    }
}

fn contains_multipoint(
    points: &[Position],
    geometry: &SpatialGeometry,
) -> Result<bool, SpatialError> {
    match geometry {
        SpatialGeometry::Point(point) => {
            Ok(points.iter().any(|candidate| candidate.xy_equals(*point)))
        }
        SpatialGeometry::MultiPoint(other_points) => Ok(other_points
            .iter()
            .all(|point| points.iter().any(|candidate| candidate.xy_equals(*point)))),
        _ => Ok(false),
    }
}

fn contains_multilinestring(
    lines: &[Vec<Position>],
    geometry: &SpatialGeometry,
) -> Result<bool, SpatialError> {
    match geometry {
        SpatialGeometry::Point(point) => {
            Ok(lines.iter().any(|line| point_on_linestring(*point, line)))
        }
        SpatialGeometry::LineString(line) => {
            Ok(lines.iter().any(|candidate| line_equals(candidate, line)))
        }
        SpatialGeometry::MultiLineString(other_lines) => Ok(other_lines
            .iter()
            .all(|line| lines.iter().any(|candidate| line_equals(candidate, line)))),
        _ => Ok(false),
    }
}

fn contains_multipolygon(
    polygons: &[Vec<Vec<Position>>],
    geometry: &SpatialGeometry,
) -> Result<bool, SpatialError> {
    match geometry {
        SpatialGeometry::Point(point) => Ok(polygons
            .iter()
            .any(|polygon| point_in_polygon(*point, polygon))),
        SpatialGeometry::Polygon(polygon) => Ok(polygons
            .iter()
            .any(|candidate| contains_polygon(candidate, polygon).unwrap_or(false))),
        SpatialGeometry::MultiPolygon(other_polygons) => Ok(other_polygons.iter().all(|polygon| {
            polygons
                .iter()
                .any(|candidate| contains_polygon(candidate, polygon).unwrap_or(false))
        })),
        _ => Ok(false),
    }
}

fn contains_linestring(polygon: &[Vec<Position>], line: &[Position]) -> Result<bool, SpatialError> {
    if line.is_empty() {
        return Err(SpatialError::InvalidInput(
            "line must contain points".to_string(),
        ));
    }

    if !line.iter().all(|point| point_in_polygon(*point, polygon)) {
        return Ok(false);
    }

    let boundary = SpatialGeometry::Polygon(polygon.to_vec());
    let as_line = SpatialGeometry::LineString(line.to_vec());
    Ok(planar_geometry_distance(&boundary, &as_line)? <= EPS || line.len() >= 2)
}

fn contains_polygon(
    container: &[Vec<Position>],
    contained: &[Vec<Position>],
) -> Result<bool, SpatialError> {
    let Some(outer) = contained.first() else {
        return Err(SpatialError::InvalidInput(
            "contained polygon must have at least one ring".to_string(),
        ));
    };
    if outer.is_empty() {
        return Err(SpatialError::InvalidInput(
            "contained polygon outer ring must not be empty".to_string(),
        ));
    }

    for point in outer {
        if !point_in_polygon(*point, container) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn point_on_linestring(point: Position, line: &[Position]) -> bool {
    line.windows(2)
        .any(|segment| point_on_segment(point, segment[0], segment[1]))
}

fn point_in_polygon(point: Position, polygon: &[Vec<Position>]) -> bool {
    if polygon.is_empty() {
        return false;
    }
    if !point_in_ring(point, &polygon[0]) {
        return false;
    }
    for hole in polygon.iter().skip(1) {
        if point_in_ring(point, hole) {
            return false;
        }
    }
    true
}

fn point_in_ring(point: Position, ring: &[Position]) -> bool {
    if ring.len() < 3 {
        return false;
    }

    let mut inside = false;
    let mut prev = ring[ring.len() - 1];
    for &current in ring {
        let intersects = ((current.y > point.y) != (prev.y > point.y))
            && (point.x
                < (prev.x - current.x) * (point.y - current.y)
                    / (prev.y - current.y + f64::EPSILON)
                    + current.x);
        if intersects {
            inside = !inside;
        }
        prev = current;
    }

    inside
}

fn point_on_segment(point: Position, start: Position, end: Position) -> bool {
    let cross = (point.y - start.y) * (end.x - start.x) - (point.x - start.x) * (end.y - start.y);
    if cross.abs() > EPS {
        return false;
    }

    let dot = (point.x - start.x) * (end.x - start.x) + (point.y - start.y) * (end.y - start.y);
    if dot < -EPS {
        return false;
    }

    let len_sq = (end.x - start.x).powi(2) + (end.y - start.y).powi(2);
    dot <= len_sq + EPS
}

fn line_equals(a: &[Position], b: &[Position]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let same_order = a.iter().zip(b.iter()).all(|(ap, bp)| ap.xy_equals(*bp));
    if same_order {
        return true;
    }

    a.iter()
        .zip(b.iter().rev())
        .all(|(ap, bp)| ap.xy_equals(*bp))
}

fn polygon_equals(a: &[Vec<Position>], b: &[Vec<Position>]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    a.iter()
        .zip(b.iter())
        .all(|(a_ring, b_ring)| ring_equals(a_ring, b_ring))
}

fn ring_equals(a: &[Position], b: &[Position]) -> bool {
    let a = ring_without_repeated_endpoint(a);
    let b = ring_without_repeated_endpoint(b);
    if a.len() != b.len() {
        return false;
    }
    if a.is_empty() {
        return true;
    }

    for start in 0..b.len() {
        let forward = (0..a.len()).all(|idx| {
            let b_idx = (start + idx) % b.len();
            a[idx].xy_equals(b[b_idx])
        });
        if forward {
            return true;
        }

        let reverse = (0..a.len()).all(|idx| {
            let b_idx = (start + b.len() - idx) % b.len();
            a[idx].xy_equals(b[b_idx])
        });
        if reverse {
            return true;
        }
    }

    false
}

fn ring_without_repeated_endpoint(ring: &[Position]) -> &[Position] {
    if ring
        .first()
        .zip(ring.last())
        .is_some_and(|(first, last)| ring.len() > 1 && first.xy_equals(*last))
    {
        &ring[..ring.len() - 1]
    } else {
        ring
    }
}

fn multipoint_equals(a: &[Position], b: &[Position]) -> Result<bool, SpatialError> {
    if a.len() != b.len() {
        return Ok(false);
    }
    Ok(a.iter().all(|ap| b.iter().any(|bp| ap.xy_equals(*bp))))
}

fn multiline_equals(a: &[Vec<Position>], b: &[Vec<Position>]) -> Result<bool, SpatialError> {
    if a.len() != b.len() {
        return Ok(false);
    }
    Ok(a.iter()
        .all(|line| b.iter().any(|candidate| line_equals(line, candidate))))
}

fn multipolygon_equals(
    a: &[Vec<Vec<Position>>],
    b: &[Vec<Vec<Position>>],
) -> Result<bool, SpatialError> {
    if a.len() != b.len() {
        return Ok(false);
    }
    Ok(a.iter()
        .all(|polygon| b.iter().any(|candidate| polygon_equals(polygon, candidate))))
}

#[cfg(test)]
mod tests {
    use crate::spatial::predicate::{contains, equals, intersects, within};
    use crate::spatial::types::{Position, SpatialGeometry};

    fn square() -> SpatialGeometry {
        SpatialGeometry::Polygon(vec![vec![
            Position::xy(0.0, 0.0),
            Position::xy(2.0, 0.0),
            Position::xy(2.0, 2.0),
            Position::xy(0.0, 0.0),
        ]])
    }

    #[test]
    fn polygon_contains_point() {
        let polygon = square();
        let point = SpatialGeometry::Point(Position::xy(1.0, 1.0));
        assert!(contains(&polygon, &point).expect("contains should succeed"));
        assert!(within(&point, &polygon).expect("within should succeed"));
    }

    #[test]
    fn lines_intersect() {
        let a = SpatialGeometry::LineString(vec![Position::xy(0.0, 0.0), Position::xy(2.0, 2.0)]);
        let b = SpatialGeometry::LineString(vec![Position::xy(0.0, 2.0), Position::xy(2.0, 0.0)]);
        assert!(intersects(&a, &b).expect("intersects should succeed"));
    }

    #[test]
    fn polygon_equality_allows_ring_rotation() {
        let a = square();
        let b = SpatialGeometry::Polygon(vec![vec![
            Position::xy(2.0, 0.0),
            Position::xy(2.0, 2.0),
            Position::xy(0.0, 0.0),
            Position::xy(2.0, 0.0),
        ]]);
        assert!(equals(&a, &b).expect("equals should succeed"));
    }
}
