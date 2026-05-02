pub fn workout_category(c: i64) -> String {
    let s = match c {
        1 => "walk",
        2 => "run",
        3 => "hike",
        4 => "skating",
        5 => "bmx",
        6 => "bicycling",
        7 => "swim",
        8 => "surfing",
        9 => "kitesurfing",
        10 => "windsurfing",
        11 => "bodyboard",
        12 => "tennis",
        13 => "table_tennis",
        14 => "squash",
        15 => "badminton",
        16 => "lift_weights",
        17 => "calisthenics",
        18 => "elliptical",
        19 => "pilates",
        20 => "basketball",
        21 => "soccer",
        22 => "football",
        23 => "rugby",
        24 => "volleyball",
        25 => "waterpolo",
        26 => "horse_riding",
        27 => "golf",
        28 => "yoga",
        29 => "dancing",
        30 => "boxing",
        31 => "fencing",
        32 => "wrestling",
        33 => "martial_arts",
        34 => "skiing",
        35 => "snowboarding",
        36 => "rowing",
        37 => "zumba",
        38 => "baseball",
        39 => "handball",
        40 => "hockey",
        41 => "ice_hockey",
        42 => "climbing",
        43 => "ice_skating",
        44 => "multi_sport",
        45 => "indoor_walk",
        46 => "indoor_run",
        47 => "indoor_bike",
        128 => "other",
        187 => "meditation",
        188 => "stretching",
        191 => "hiit",
        192 => "scuba_diving",
        272 => "snorkeling",
        306 => "rugby_union",
        307 => "rugby_league",
        _ => return format!("unknown_{c}"),
    };
    s.to_string()
}

pub fn measure_attrib(a: i64) -> &'static str {
    match a {
        0 => "device_owner",
        1 => "device_other",
        2 => "manual",
        4 => "manual_creation",
        5 => "device_user_associated",
        7 => "auto",
        8 => "manual_user_pending",
        15 => "ambiguous",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_workout_categories() {
        assert_eq!(workout_category(1), "walk");
        assert_eq!(workout_category(7), "swim");
        assert_eq!(workout_category(46), "indoor_run");
        assert_eq!(workout_category(128), "other");
    }

    #[test]
    fn unknown_workout_category_keeps_int() {
        assert_eq!(workout_category(9999), "unknown_9999");
    }

    #[test]
    fn known_attribs() {
        assert_eq!(measure_attrib(0), "device_owner");
        assert_eq!(measure_attrib(7), "auto");
        assert_eq!(measure_attrib(2), "manual");
    }

    #[test]
    fn unknown_attrib() {
        assert_eq!(measure_attrib(99), "unknown");
    }
}
