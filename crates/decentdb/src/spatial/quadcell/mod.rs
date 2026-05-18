#![allow(dead_code)]

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct QuadCellId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Bounds {
    pub(crate) min_x: f64,
    pub(crate) min_y: f64,
    pub(crate) max_x: f64,
    pub(crate) max_y: f64,
}

impl Bounds {
    pub(crate) fn normalized(self) -> Self {
        Self {
            min_x: self.min_x.min(self.max_x),
            min_y: self.min_y.min(self.max_y),
            max_x: self.min_x.max(self.max_x),
            max_y: self.min_y.max(self.max_y),
        }
    }
}

pub(crate) fn point_cell_id(x: f64, y: f64, level: u8) -> QuadCellId {
    let level = level.min(30);
    let scale = 1u64 << level;
    let xi = (((x + 1.0) * 0.5) * scale as f64)
        .floor()
        .clamp(0.0, (scale.saturating_sub(1)) as f64) as u64;
    let yi = (((y + 1.0) * 0.5) * scale as f64)
        .floor()
        .clamp(0.0, (scale.saturating_sub(1)) as f64) as u64;
    QuadCellId((xi << 32) | yi)
}

pub(crate) fn covering(bounds: Bounds, level: u8) -> Vec<QuadCellId> {
    let b = bounds.normalized();
    vec![
        point_cell_id(b.min_x, b.min_y, level),
        point_cell_id(b.min_x, b.max_y, level),
        point_cell_id(b.max_x, b.min_y, level),
        point_cell_id(b.max_x, b.max_y, level),
    ]
}

pub(crate) fn min_distance_to_cell(x: f64, y: f64, cell_bounds: Bounds) -> f64 {
    let b = cell_bounds.normalized();
    let dx = if x < b.min_x {
        b.min_x - x
    } else if x > b.max_x {
        x - b.max_x
    } else {
        0.0
    };

    let dy = if y < b.min_y {
        b.min_y - y
    } else if y > b.max_y {
        y - b.max_y
    } else {
        0.0
    };

    (dx * dx + dy * dy).sqrt()
}

#[cfg(test)]
mod tests {
    use crate::spatial::quadcell::{covering, min_distance_to_cell, Bounds};

    #[test]
    fn normalized_bounds_sorts_axes() {
        let bounds = Bounds {
            min_x: 2.0,
            min_y: 4.0,
            max_x: 1.0,
            max_y: 3.0,
        }
        .normalized();

        assert_eq!(bounds.min_x, 1.0);
        assert_eq!(bounds.max_y, 4.0);
    }

    #[test]
    fn covering_is_non_empty() {
        let cells = covering(
            Bounds {
                min_x: -0.1,
                min_y: -0.1,
                max_x: 0.1,
                max_y: 0.1,
            },
            8,
        );
        assert_eq!(cells.len(), 4);
    }

    #[test]
    fn min_distance_zero_for_inside_point() {
        let bounds = Bounds {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 1.0,
            max_y: 1.0,
        };
        assert_eq!(min_distance_to_cell(0.5, 0.5, bounds), 0.0);
    }
}
