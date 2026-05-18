#![allow(dead_code)]

use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct S2CellId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct LonLat {
    pub(crate) lon_deg: f64,
    pub(crate) lat_deg: f64,
}

pub(crate) fn point_cell_id(point: LonLat, level: u8) -> S2CellId {
    let level = level.min(30);
    let scale = 1u64 << level;
    let x = (((point.lon_deg + 180.0) / 360.0) * scale as f64)
        .floor()
        .clamp(0.0, (scale.saturating_sub(1)) as f64) as u64;
    let y = (((point.lat_deg + 90.0) / 180.0) * scale as f64)
        .floor()
        .clamp(0.0, (scale.saturating_sub(1)) as f64) as u64;
    S2CellId(morton_interleave(x, y))
}

pub(crate) fn covering_for_envelope(
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
    level: u8,
) -> Vec<S2CellId> {
    let mut cells = BTreeSet::new();
    for corner in [
        LonLat {
            lon_deg: min_lon,
            lat_deg: min_lat,
        },
        LonLat {
            lon_deg: min_lon,
            lat_deg: max_lat,
        },
        LonLat {
            lon_deg: max_lon,
            lat_deg: min_lat,
        },
        LonLat {
            lon_deg: max_lon,
            lat_deg: max_lat,
        },
    ] {
        cells.insert(point_cell_id(corner, level));
    }
    cells.into_iter().collect()
}

pub(crate) fn min_distance_to_cell(query: LonLat, cell: S2CellId, level: u8) -> f64 {
    let (x, y) = morton_deinterleave(cell.0);
    let scale = 1u64 << level.min(30);
    let cell_center = LonLat {
        lon_deg: (x as f64 + 0.5) * 360.0 / scale as f64 - 180.0,
        lat_deg: (y as f64 + 0.5) * 180.0 / scale as f64 - 90.0,
    };
    let dx = query.lon_deg - cell_center.lon_deg;
    let dy = query.lat_deg - cell_center.lat_deg;
    (dx * dx + dy * dy).sqrt()
}

fn morton_interleave(x: u64, y: u64) -> u64 {
    let mut result = 0u64;
    for i in 0..32 {
        result |= ((x >> i) & 1) << (2 * i);
        result |= ((y >> i) & 1) << (2 * i + 1);
    }
    result
}

fn morton_deinterleave(code: u64) -> (u64, u64) {
    let mut x = 0u64;
    let mut y = 0u64;
    for i in 0..32 {
        x |= ((code >> (2 * i)) & 1) << i;
        y |= ((code >> (2 * i + 1)) & 1) << i;
    }
    (x, y)
}

#[cfg(test)]
mod tests {
    use crate::spatial::s2::{covering_for_envelope, min_distance_to_cell, point_cell_id, LonLat};

    #[test]
    fn point_cell_is_stable() {
        let point = LonLat {
            lon_deg: -97.7431,
            lat_deg: 30.2672,
        };
        let cell = point_cell_id(point, 12);
        assert!(cell.0 > 0);
    }

    #[test]
    fn envelope_covering_is_non_empty() {
        let cells = covering_for_envelope(-1.0, -1.0, 1.0, 1.0, 10);
        assert!(!cells.is_empty());
    }

    #[test]
    fn min_distance_non_negative() {
        let point = LonLat {
            lon_deg: 0.0,
            lat_deg: 0.0,
        };
        let cell = point_cell_id(point, 8);
        assert!(min_distance_to_cell(point, cell, 8) >= 0.0);
    }
}
