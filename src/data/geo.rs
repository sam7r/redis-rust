use crate::data::types::{GeoSearchOption, GeoUnit};

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

pub fn validate_coordinates(latitude: f64, longitude: f64) -> Result<(), String> {
    if !(MIN_LATITUDE..=MAX_LATITUDE).contains(&latitude) {
        return Err(format!(
            "Latitude must be between {} and {}",
            MIN_LATITUDE, MAX_LATITUDE
        ));
    }
    if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&longitude) {
        return Err(format!(
            "Longitude must be between {} and {}",
            MIN_LONGITUDE, MAX_LONGITUDE
        ));
    }
    Ok(())
}

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

pub fn encode(latitude: f64, longitude: f64) -> u64 {
    // Normalize to the range 0-2^26
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Truncate to integers
    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}

#[derive(Debug, Clone, Copy)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32 // Cast to u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> Coordinates {
    // Calculate the grid boundaries
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

    // Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    Coordinates {
        latitude,
        longitude,
    }
}

pub fn decode(geo_code: u64) -> Coordinates {
    // Align bits of both latitude and longitude to take even-numbered position
    let y = geo_code >> 1;
    let x = geo_code;

    // Compact bits back to 32-bit ints
    let grid_latitude_number = compact_int64_to_int32(x);
    let grid_longitude_number = compact_int64_to_int32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

pub fn haversine_distance(coord1: &Coordinates, coord2: &Coordinates, unit: GeoUnit) -> f64 {
    let lat1_rad = coord1.latitude.to_radians();
    let lat2_rad = coord2.latitude.to_radians();
    let delta_lat = (coord2.latitude - coord1.latitude).to_radians();
    let delta_lon = (coord2.longitude - coord1.longitude).to_radians();

    let v = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);

    match unit {
        GeoUnit::M => 6372797.560856 * 2.0 * v.sqrt().asin(),
        GeoUnit::KM => 6372.79756085 * 2.0 * v.sqrt().asin(),
        GeoUnit::MI => 3959.87281827 * 2.0 * v.sqrt().asin(),
        GeoUnit::FT => 20908128.480498 * 2.0 * v.sqrt().asin(),
    }
}

pub fn convert_unit_to_meters(value: f64, unit: GeoUnit) -> f64 {
    match unit {
        GeoUnit::M => value,
        GeoUnit::KM => value * 1000.0,
        GeoUnit::MI => value * 1609.344,
        GeoUnit::FT => value * 0.3048,
    }
}

pub fn convert_meters_to_unit(value: f64, unit: GeoUnit) -> f64 {
    match unit {
        GeoUnit::M => value,
        GeoUnit::KM => value / 1000.0,
        GeoUnit::MI => value / 1609.344,
        GeoUnit::FT => value / 0.3048,
    }
}

pub fn max_distance_bybox_or_radius(options: Vec<GeoSearchOption>) -> Option<f64> {
    let by_radius = options.iter().find_map(|opt| {
        if let GeoSearchOption::BYRADIUS(r, u) = opt {
            Some((*r, u.clone()))
        } else {
            None
        }
    });

    let by_box = options.iter().find_map(|opt| {
        if let GeoSearchOption::BYBOX(w, h, u) = opt {
            Some((*w, *h, u.clone()))
        } else {
            None
        }
    });

    if let Some((radius, unit)) = by_radius {
        Some(convert_unit_to_meters(radius, unit))
    } else if let Some((width, height, unit)) = by_box {
        let width_meters = convert_unit_to_meters(width, unit.clone());
        let height_meters = convert_unit_to_meters(height, unit.clone());
        Some((width_meters / 2.0).hypot(height_meters / 2.0))
    } else {
        None
    }
}
