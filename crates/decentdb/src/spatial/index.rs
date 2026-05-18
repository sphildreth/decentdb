use std::collections::{BTreeMap, BTreeSet};

use crate::spatial::distance::EARTH_RADIUS_METERS;
use crate::spatial::quadcell::QuadCellId;
use crate::spatial::s2::S2CellId;
use crate::spatial::types::{SpatialError, SpatialValue};

const GEOGRAPHY_LEVEL: u8 = 8;
const GEOMETRY_CELL_SIZE: f64 = 1.0;
const MAX_COVERING_CELLS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpatialIndexBackend {
    GeographyS2,
    GeometryQuadCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SpatialCell {
    S2(S2CellId),
    Quad(QuadCellId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpatialIndexKey {
    pub(crate) backend: SpatialIndexBackend,
    pub(crate) primary_cell: SpatialCell,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SpatialEnvelope {
    pub(crate) min_x: f64,
    pub(crate) min_y: f64,
    pub(crate) max_x: f64,
    pub(crate) max_y: f64,
}

impl SpatialEnvelope {
    pub(crate) fn from_value(value: &SpatialValue) -> Result<Self, SpatialError> {
        let mut positions = value.geometry.all_positions();
        let first = positions.next().ok_or_else(|| {
            SpatialError::InvalidInput("spatial value has no coordinates".to_string())
        })?;
        let mut envelope = Self {
            min_x: first.x,
            min_y: first.y,
            max_x: first.x,
            max_y: first.y,
        };
        for position in positions {
            envelope.min_x = envelope.min_x.min(position.x);
            envelope.min_y = envelope.min_y.min(position.y);
            envelope.max_x = envelope.max_x.max(position.x);
            envelope.max_y = envelope.max_y.max(position.y);
        }
        if !envelope.is_finite() {
            return Err(SpatialError::InvalidInput(
                "spatial envelope contains non-finite coordinates".to_string(),
            ));
        }
        Ok(envelope)
    }

    pub(crate) fn intersects(self, other: Self) -> bool {
        self.min_x <= other.max_x
            && self.max_x >= other.min_x
            && self.min_y <= other.max_y
            && self.max_y >= other.min_y
    }

    pub(crate) fn expand_planar(self, distance: f64) -> Self {
        let distance = distance.max(0.0);
        Self {
            min_x: self.min_x - distance,
            min_y: self.min_y - distance,
            max_x: self.max_x + distance,
            max_y: self.max_y + distance,
        }
    }

    pub(crate) fn expand_geography_meters(self, meters: f64) -> Self {
        let meters = meters.max(0.0);
        let lat_delta = meters.to_degrees() / EARTH_RADIUS_METERS;
        let center_lat = ((self.min_y + self.max_y) * 0.5).to_radians();
        let lon_delta = if center_lat.cos().abs() < 1e-12 {
            180.0
        } else {
            (meters / (EARTH_RADIUS_METERS * center_lat.cos().abs())).to_degrees()
        };
        Self {
            min_x: (self.min_x - lon_delta).max(-180.0),
            min_y: (self.min_y - lat_delta).max(-90.0),
            max_x: (self.max_x + lon_delta).min(180.0),
            max_y: (self.max_y + lat_delta).min(90.0),
        }
    }

    fn is_finite(self) -> bool {
        self.min_x.is_finite()
            && self.min_y.is_finite()
            && self.max_x.is_finite()
            && self.max_y.is_finite()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpatialIndexEntry {
    pub(crate) row_id: i64,
    pub(crate) envelope: SpatialEnvelope,
    pub(crate) value: SpatialValue,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpatialRuntimeIndex {
    backend: SpatialIndexBackend,
    entries: BTreeMap<i64, SpatialIndexEntry>,
    cells: BTreeMap<SpatialCell, Vec<i64>>,
    all_row_ids: Vec<i64>,
}

impl SpatialRuntimeIndex {
    pub(crate) fn new(backend: SpatialIndexBackend) -> Self {
        Self {
            backend,
            entries: BTreeMap::new(),
            cells: BTreeMap::new(),
            all_row_ids: Vec::new(),
        }
    }

    pub(crate) fn backend(&self) -> SpatialIndexBackend {
        self.backend
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn insert(&mut self, row_id: i64, value: SpatialValue) -> Result<(), SpatialError> {
        self.remove(row_id);
        let envelope = SpatialEnvelope::from_value(&value)?;
        let entry = SpatialIndexEntry {
            row_id,
            envelope,
            value,
        };
        if let Some(cells) = covering_cells(envelope, self.backend) {
            for cell in cells {
                let row_ids = self.cells.entry(cell).or_default();
                if !row_ids.contains(&row_id) {
                    row_ids.push(row_id);
                }
            }
        }
        self.all_row_ids.push(row_id);
        self.all_row_ids.sort_unstable();
        self.all_row_ids.dedup();
        self.entries.insert(row_id, entry);
        Ok(())
    }

    pub(crate) fn remove(&mut self, row_id: i64) -> Option<SpatialIndexEntry> {
        let entry = self.entries.remove(&row_id)?;
        self.all_row_ids.retain(|candidate| *candidate != row_id);
        let empty_cells = self
            .cells
            .iter_mut()
            .filter_map(|(cell, row_ids)| {
                row_ids.retain(|candidate| *candidate != row_id);
                row_ids.is_empty().then_some(*cell)
            })
            .collect::<Vec<_>>();
        for cell in empty_cells {
            self.cells.remove(&cell);
        }
        Some(entry)
    }

    pub(crate) fn candidate_row_ids(&self, envelope: SpatialEnvelope) -> Vec<i64> {
        let Some(cells) = covering_cells(envelope, self.backend) else {
            return self
                .all_row_ids
                .iter()
                .copied()
                .filter(|row_id| {
                    self.entries
                        .get(row_id)
                        .is_some_and(|entry| entry.envelope.intersects(envelope))
                })
                .collect();
        };
        let mut row_ids = BTreeSet::new();
        for cell in cells {
            if let Some(candidates) = self.cells.get(&cell) {
                row_ids.extend(candidates.iter().copied());
            }
        }
        row_ids
            .into_iter()
            .filter(|row_id| {
                self.entries
                    .get(row_id)
                    .is_some_and(|entry| entry.envelope.intersects(envelope))
            })
            .collect()
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &SpatialIndexEntry> {
        self.entries.values()
    }
}

pub(crate) fn candidate_cells_for_value(
    value: &SpatialValue,
    backend: SpatialIndexBackend,
) -> Result<Vec<SpatialCell>, SpatialError> {
    let envelope = SpatialEnvelope::from_value(value)?;
    Ok(covering_cells(envelope, backend).unwrap_or_default())
}

pub(crate) fn candidate_cells_for_envelope(
    envelope: SpatialEnvelope,
    backend: SpatialIndexBackend,
) -> Option<Vec<SpatialCell>> {
    covering_cells(envelope, backend)
}

fn covering_cells(
    envelope: SpatialEnvelope,
    backend: SpatialIndexBackend,
) -> Option<Vec<SpatialCell>> {
    match backend {
        SpatialIndexBackend::GeographyS2 => geography_cells(envelope),
        SpatialIndexBackend::GeometryQuadCell => geometry_cells(envelope),
    }
}

fn geography_cells(envelope: SpatialEnvelope) -> Option<Vec<SpatialCell>> {
    let scale = 1u64 << GEOGRAPHY_LEVEL;
    let min_x = geography_x_cell(envelope.min_x, scale);
    let max_x = geography_x_cell(envelope.max_x, scale);
    let min_y = geography_y_cell(envelope.min_y, scale);
    let max_y = geography_y_cell(envelope.max_y, scale);
    let count = cell_count(min_x, max_x, min_y, max_y)?;
    if count > MAX_COVERING_CELLS {
        return None;
    }
    let mut cells = Vec::with_capacity(count);
    for x in min_x..=max_x {
        for y in min_y..=max_y {
            cells.push(SpatialCell::S2(S2CellId(morton_interleave(x, y))));
        }
    }
    Some(cells)
}

fn geometry_cells(envelope: SpatialEnvelope) -> Option<Vec<SpatialCell>> {
    let min_x = geometry_cell_coord(envelope.min_x);
    let max_x = geometry_cell_coord(envelope.max_x);
    let min_y = geometry_cell_coord(envelope.min_y);
    let max_y = geometry_cell_coord(envelope.max_y);
    let count = cell_count_i64(min_x, max_x, min_y, max_y)?;
    if count > MAX_COVERING_CELLS {
        return None;
    }
    let mut cells = Vec::with_capacity(count);
    for x in min_x..=max_x {
        for y in min_y..=max_y {
            cells.push(SpatialCell::Quad(QuadCellId(pack_signed_cell(x, y))));
        }
    }
    Some(cells)
}

fn geography_x_cell(lon: f64, scale: u64) -> u64 {
    (((lon.clamp(-180.0, 180.0) + 180.0) / 360.0) * scale as f64)
        .floor()
        .clamp(0.0, (scale.saturating_sub(1)) as f64) as u64
}

fn geography_y_cell(lat: f64, scale: u64) -> u64 {
    (((lat.clamp(-90.0, 90.0) + 90.0) / 180.0) * scale as f64)
        .floor()
        .clamp(0.0, (scale.saturating_sub(1)) as f64) as u64
}

fn geometry_cell_coord(value: f64) -> i64 {
    (value / GEOMETRY_CELL_SIZE).floor() as i64
}

fn pack_signed_cell(x: i64, y: i64) -> u64 {
    let x = i32::try_from(x.clamp(i64::from(i32::MIN), i64::from(i32::MAX))).unwrap_or(if x < 0 {
        i32::MIN
    } else {
        i32::MAX
    }) as u32;
    let y = i32::try_from(y.clamp(i64::from(i32::MIN), i64::from(i32::MAX))).unwrap_or(if y < 0 {
        i32::MIN
    } else {
        i32::MAX
    }) as u32;
    (u64::from(x) << 32) | u64::from(y)
}

fn cell_count(min_x: u64, max_x: u64, min_y: u64, max_y: u64) -> Option<usize> {
    let width = max_x.checked_sub(min_x)?.checked_add(1)?;
    let height = max_y.checked_sub(min_y)?.checked_add(1)?;
    usize::try_from(width.checked_mul(height)?).ok()
}

fn cell_count_i64(min_x: i64, max_x: i64, min_y: i64, max_y: i64) -> Option<usize> {
    let width = max_x.checked_sub(min_x)?.checked_add(1)?;
    let height = max_y.checked_sub(min_y)?.checked_add(1)?;
    usize::try_from(width.checked_mul(height)?).ok()
}

fn morton_interleave(x: u64, y: u64) -> u64 {
    let mut result = 0u64;
    for i in 0..32 {
        result |= ((x >> i) & 1) << (2 * i);
        result |= ((y >> i) & 1) << (2 * i + 1);
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::spatial::index::{
        candidate_cells_for_value, SpatialEnvelope, SpatialIndexBackend, SpatialRuntimeIndex,
    };
    use crate::spatial::types::{CoordinateDimensions, Position, SpatialGeometry, SpatialValue};

    #[test]
    fn candidate_cells_are_non_empty_for_point() {
        let value = SpatialValue::new(
            4326,
            CoordinateDimensions::Xy,
            SpatialGeometry::Point(Position::xy(0.0, 0.0)),
        )
        .expect("valid point");

        let cells = candidate_cells_for_value(&value, SpatialIndexBackend::GeographyS2)
            .expect("candidate call should succeed");
        assert!(!cells.is_empty());
    }

    #[test]
    fn runtime_index_prunes_by_envelope() {
        let near = SpatialValue::new(
            0,
            CoordinateDimensions::Xy,
            SpatialGeometry::Point(Position::xy(1.0, 1.0)),
        )
        .expect("valid point");
        let far = SpatialValue::new(
            0,
            CoordinateDimensions::Xy,
            SpatialGeometry::Point(Position::xy(10.0, 10.0)),
        )
        .expect("valid point");

        let mut index = SpatialRuntimeIndex::new(SpatialIndexBackend::GeometryQuadCell);
        index.insert(1, near).expect("insert near");
        index.insert(2, far).expect("insert far");

        let ids = index.candidate_row_ids(SpatialEnvelope {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 2.0,
            max_y: 2.0,
        });
        assert_eq!(ids, vec![1]);
    }
}
