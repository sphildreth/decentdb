use crate::spatial::types::{Position, SpatialError, SpatialGeometry};

pub(crate) const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

pub(crate) fn geography_point_distance_meters(a: Position, b: Position) -> f64 {
    let lat1 = a.y.to_radians();
    let lat2 = b.y.to_radians();
    let dlat = (b.y - a.y).to_radians();
    let dlon = (b.x - a.x).to_radians();

    let sin_dlat = (dlat * 0.5).sin();
    let sin_dlon = (dlon * 0.5).sin();
    let h = sin_dlat * sin_dlat + lat1.cos() * lat2.cos() * sin_dlon * sin_dlon;
    2.0 * EARTH_RADIUS_METERS * h.sqrt().atan2((1.0 - h).sqrt())
}

pub(crate) fn planar_geometry_distance(
    a: &SpatialGeometry,
    b: &SpatialGeometry,
) -> Result<f64, SpatialError> {
    match (a, b) {
        (SpatialGeometry::Point(pa), SpatialGeometry::Point(pb)) => Ok(point_distance_xy(*pa, *pb)),
        (SpatialGeometry::Point(point), SpatialGeometry::LineString(line))
        | (SpatialGeometry::LineString(line), SpatialGeometry::Point(point)) => {
            point_to_linestring_distance(*point, line)
        }
        (SpatialGeometry::Point(point), SpatialGeometry::Polygon(polygon))
        | (SpatialGeometry::Polygon(polygon), SpatialGeometry::Point(point)) => {
            point_to_polygon_distance(*point, polygon)
        }
        (SpatialGeometry::LineString(a_line), SpatialGeometry::LineString(b_line)) => {
            linestring_distance(a_line, b_line)
        }
        (SpatialGeometry::LineString(line), SpatialGeometry::Polygon(polygon))
        | (SpatialGeometry::Polygon(polygon), SpatialGeometry::LineString(line)) => {
            linestring_to_polygon_distance(line, polygon)
        }
        (SpatialGeometry::Polygon(a_polygon), SpatialGeometry::Polygon(b_polygon)) => {
            polygon_to_polygon_distance(a_polygon, b_polygon)
        }
        (SpatialGeometry::MultiPoint(points), geom)
        | (geom, SpatialGeometry::MultiPoint(points)) => {
            min_distance_points_to_geometry(points, geom)
        }
        (SpatialGeometry::MultiLineString(lines), geom)
        | (geom, SpatialGeometry::MultiLineString(lines)) => {
            min_distance_lines_to_geometry(lines, geom)
        }
        (SpatialGeometry::MultiPolygon(polygons), geom)
        | (geom, SpatialGeometry::MultiPolygon(polygons)) => {
            min_distance_polygons_to_geometry(polygons, geom)
        }
    }
}

pub(crate) fn point_distance_xy(a: Position, b: Position) -> f64 {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    (dx * dx + dy * dy).sqrt()
}

fn min_distance_points_to_geometry(
    points: &[Position],
    geometry: &SpatialGeometry,
) -> Result<f64, SpatialError> {
    if points.is_empty() {
        return Err(SpatialError::InvalidInput(
            "empty multipoint has no distance semantics".to_string(),
        ));
    }

    let mut min = f64::INFINITY;
    for point in points {
        let d = planar_geometry_distance(&SpatialGeometry::Point(*point), geometry)?;
        min = min.min(d);
    }
    Ok(min)
}

fn min_distance_lines_to_geometry(
    lines: &[Vec<Position>],
    geometry: &SpatialGeometry,
) -> Result<f64, SpatialError> {
    if lines.is_empty() {
        return Err(SpatialError::InvalidInput(
            "empty multilinestring has no distance semantics".to_string(),
        ));
    }

    let mut min = f64::INFINITY;
    for line in lines {
        let d = planar_geometry_distance(&SpatialGeometry::LineString(line.clone()), geometry)?;
        min = min.min(d);
    }
    Ok(min)
}

fn min_distance_polygons_to_geometry(
    polygons: &[Vec<Vec<Position>>],
    geometry: &SpatialGeometry,
) -> Result<f64, SpatialError> {
    if polygons.is_empty() {
        return Err(SpatialError::InvalidInput(
            "empty multipolygon has no distance semantics".to_string(),
        ));
    }

    let mut min = f64::INFINITY;
    for polygon in polygons {
        let d = planar_geometry_distance(&SpatialGeometry::Polygon(polygon.clone()), geometry)?;
        min = min.min(d);
    }
    Ok(min)
}

fn point_to_linestring_distance(point: Position, line: &[Position]) -> Result<f64, SpatialError> {
    if line.is_empty() {
        return Err(SpatialError::InvalidInput(
            "linestring must contain at least one point".to_string(),
        ));
    }
    if line.len() == 1 {
        return Ok(point_distance_xy(point, line[0]));
    }

    let mut min = f64::INFINITY;
    for segment in line.windows(2) {
        min = min.min(point_to_segment_distance(point, segment[0], segment[1]));
    }
    Ok(min)
}

fn point_to_polygon_distance(
    point: Position,
    polygon: &[Vec<Position>],
) -> Result<f64, SpatialError> {
    if polygon.is_empty() {
        return Err(SpatialError::InvalidInput(
            "polygon must contain at least one ring".to_string(),
        ));
    }
    if point_in_polygon(point, polygon) {
        return Ok(0.0);
    }

    let mut min = f64::INFINITY;
    for ring in polygon {
        min = min.min(point_to_ring_distance(point, ring)?);
    }
    Ok(min)
}

fn point_to_ring_distance(point: Position, ring: &[Position]) -> Result<f64, SpatialError> {
    if ring.len() < 2 {
        return Err(SpatialError::InvalidInput(
            "ring must contain at least two points".to_string(),
        ));
    }

    let mut min = f64::INFINITY;
    for segment in ring.windows(2) {
        min = min.min(point_to_segment_distance(point, segment[0], segment[1]));
    }
    if !ring
        .first()
        .zip(ring.last())
        .is_some_and(|(a, b)| a.xy_equals(*b))
    {
        min = min.min(point_to_segment_distance(
            point,
            *ring.last().expect("checked non-empty ring"),
            ring[0],
        ));
    }
    Ok(min)
}

fn linestring_distance(a: &[Position], b: &[Position]) -> Result<f64, SpatialError> {
    if a.is_empty() || b.is_empty() {
        return Err(SpatialError::InvalidInput(
            "linestrings must contain points".to_string(),
        ));
    }
    if a.len() == 1 && b.len() == 1 {
        return Ok(point_distance_xy(a[0], b[0]));
    }

    if linestrings_intersect(a, b) {
        return Ok(0.0);
    }

    let mut min = f64::INFINITY;
    for point in a {
        min = min.min(point_to_linestring_distance(*point, b)?);
    }
    for point in b {
        min = min.min(point_to_linestring_distance(*point, a)?);
    }
    Ok(min)
}

fn linestring_to_polygon_distance(
    line: &[Position],
    polygon: &[Vec<Position>],
) -> Result<f64, SpatialError> {
    if line.is_empty() {
        return Err(SpatialError::InvalidInput(
            "linestring must contain points".to_string(),
        ));
    }
    if polygon.is_empty() {
        return Err(SpatialError::InvalidInput(
            "polygon must contain rings".to_string(),
        ));
    }

    for point in line {
        if point_in_polygon(*point, polygon) {
            return Ok(0.0);
        }
    }

    for ring in polygon {
        if linestrings_intersect(line, ring) {
            return Ok(0.0);
        }
    }

    let mut min = f64::INFINITY;
    for point in line {
        min = min.min(point_to_polygon_distance(*point, polygon)?);
    }
    for ring in polygon {
        for point in ring {
            min = min.min(point_to_linestring_distance(*point, line)?);
        }
    }

    Ok(min)
}

fn polygon_to_polygon_distance(
    a: &[Vec<Position>],
    b: &[Vec<Position>],
) -> Result<f64, SpatialError> {
    if a.is_empty() || b.is_empty() {
        return Err(SpatialError::InvalidInput(
            "polygon must contain at least one ring".to_string(),
        ));
    }

    for point in &a[0] {
        if point_in_polygon(*point, b) {
            return Ok(0.0);
        }
    }
    for point in &b[0] {
        if point_in_polygon(*point, a) {
            return Ok(0.0);
        }
    }

    for ring_a in a {
        for ring_b in b {
            if linestrings_intersect(ring_a, ring_b) {
                return Ok(0.0);
            }
        }
    }

    let mut min = f64::INFINITY;
    for ring in a {
        for point in ring {
            min = min.min(point_to_polygon_distance(*point, b)?);
        }
    }
    for ring in b {
        for point in ring {
            min = min.min(point_to_polygon_distance(*point, a)?);
        }
    }

    Ok(min)
}

fn point_to_segment_distance(point: Position, start: Position, end: Position) -> f64 {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    let len_sq = dx * dx + dy * dy;

    if len_sq == 0.0 {
        return point_distance_xy(point, start);
    }

    let t = ((point.x - start.x) * dx + (point.y - start.y) * dy) / len_sq;
    if t <= 0.0 {
        return point_distance_xy(point, start);
    }
    if t >= 1.0 {
        return point_distance_xy(point, end);
    }

    let proj = Position::xy(start.x + t * dx, start.y + t * dy);
    point_distance_xy(point, proj)
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

fn linestrings_intersect(a: &[Position], b: &[Position]) -> bool {
    if a.len() < 2 || b.len() < 2 {
        return false;
    }
    for a_seg in a.windows(2) {
        for b_seg in b.windows(2) {
            if segments_intersect(a_seg[0], a_seg[1], b_seg[0], b_seg[1]) {
                return true;
            }
        }
    }
    false
}

fn segments_intersect(a1: Position, a2: Position, b1: Position, b2: Position) -> bool {
    fn orient(p: Position, q: Position, r: Position) -> f64 {
        (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y)
    }

    fn on_segment(p: Position, q: Position, r: Position) -> bool {
        q.x <= p.x.max(r.x) + f64::EPSILON
            && q.x + f64::EPSILON >= p.x.min(r.x)
            && q.y <= p.y.max(r.y) + f64::EPSILON
            && q.y + f64::EPSILON >= p.y.min(r.y)
    }

    let o1 = orient(a1, a2, b1);
    let o2 = orient(a1, a2, b2);
    let o3 = orient(b1, b2, a1);
    let o4 = orient(b1, b2, a2);

    if (o1 > 0.0) != (o2 > 0.0) && (o3 > 0.0) != (o4 > 0.0) {
        return true;
    }

    (o1.abs() <= f64::EPSILON && on_segment(a1, b1, a2))
        || (o2.abs() <= f64::EPSILON && on_segment(a1, b2, a2))
        || (o3.abs() <= f64::EPSILON && on_segment(b1, a1, b2))
        || (o4.abs() <= f64::EPSILON && on_segment(b1, a2, b2))
}

#[cfg(test)]
mod tests {
    use crate::spatial::distance::{geography_point_distance_meters, planar_geometry_distance};
    use crate::spatial::types::{Position, SpatialGeometry};

    #[test]
    fn geography_point_distance_is_spherical() {
        let a = Position::xy(0.0, 0.0);
        let b = Position::xy(0.0, 1.0);
        let distance = geography_point_distance_meters(a, b);

        assert!(distance > 100_000.0);
        assert!(distance < 112_500.0);
    }

    #[test]
    fn planar_point_to_line_distance() {
        let point = SpatialGeometry::Point(Position::xy(1.0, 1.0));
        let line =
            SpatialGeometry::LineString(vec![Position::xy(0.0, 0.0), Position::xy(2.0, 0.0)]);
        let distance = planar_geometry_distance(&point, &line).expect("distance should compute");

        assert!((distance - 1.0).abs() < 1e-9);
    }

    #[test]
    fn planar_polygon_distance_zero_when_inside() {
        let point = SpatialGeometry::Point(Position::xy(0.5, 0.5));
        let polygon = SpatialGeometry::Polygon(vec![vec![
            Position::xy(0.0, 0.0),
            Position::xy(1.0, 0.0),
            Position::xy(1.0, 1.0),
            Position::xy(0.0, 0.0),
        ]]);

        let distance = planar_geometry_distance(&point, &polygon).expect("distance should compute");
        assert_eq!(distance, 0.0);
    }
}
