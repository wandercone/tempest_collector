def degrees_to_cardinal(degrees: int) -> str:
    """Convert a wind bearing in degrees (0 - 360) to the nearest 16-point cardinal label."""
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(degrees / 22.5) % 16]


def derive_conditions_icon(obs: dict) -> tuple[str, str]:
    """Derive a (conditions, icon) pair from raw sensor data without calling the forecast API.

    Priority cascade: lightning → active precip → recent precip → fog → wind → sky condition.
    At night, cloud cover cannot be determined from sensors; defaults to ("Clear", "clear-night").
    Returns a (conditions_string, icon_slug) tuple.
    """
    precip        = obs.get("precip") or 0
    precip_1hr    = obs.get("precip_accum_last_1hr") or 0
    lightning     = obs.get("lightning_strike_count") or 0
    lightning_1hr = obs.get("lightning_strike_count_last_1hr") or 0
    brightness    = obs.get("brightness") or 0
    uv            = obs.get("uv") or 0
    wind_avg      = obs.get("wind_avg") or 0
    humidity      = obs.get("relative_humidity") or 0
    air_temp      = obs.get("air_temperature")
    delta_t       = obs.get("delta_t")

    is_day = uv > 0 or brightness > 500
    tod    = "day" if is_day else "night"

    # Precipitation type by temperature
    if air_temp is None or air_temp > 3:
        ptype = "rain"
    elif air_temp <= 0:
        ptype = "snow"
    else:
        ptype = "mix"

    # --- Lightning (highest priority) ---
    if lightning > 0:
        return "Thunderstorms Likely", f"possibly-thunderstorm-{tod}"
    if lightning_1hr > 10:
        return "Thunderstorms Likely", f"possibly-thunderstorm-{tod}"
    if lightning_1hr > 0:
        return "Thunderstorms Possible", f"possibly-thunderstorm-{tod}"

    # --- Active precipitation ---
    if precip > 0:
        if ptype == "snow":
            return "Snow Likely", "snow"
        if ptype == "mix":
            return "Wintry Mix Likely", "sleet"
        # Rain intensity: precip is mm/min
        if precip >= 1.67:
            return "Extreme Rain",    "rainy"
        if precip >= 0.83:
            return "Very Heavy Rain", "rainy"
        if precip >= 0.17:
            return "Heavy Rain",      "rainy"
        if precip >= 0.04:
            return "Moderate Rain",   "rainy"
        if precip >= 0.005:
            return "Light Rain",      "rainy"
        return "Very Light Rain",     "rainy"

    # --- Recent precipitation (no longer active) ---
    if precip_1hr >= 2:
        if ptype == "snow":
            return "Snow Likely", "snow"
        if ptype == "mix":
            return "Wintry Mix Likely", "sleet"
        return "Rain Likely", f"possibly-rainy-{tod}"
    if precip_1hr >= 0.5:
        if ptype == "snow":
            return "Snow Possible", f"possibly-snow-{tod}"
        if ptype == "mix":
            return "Wintry Mix Possible", f"possibly-sleet-{tod}"
        return "Rain Possible", f"possibly-rainy-{tod}"

    # --- Fog: saturated air, calm wind ---
    if humidity >= 95 and wind_avg <= 1.5 and delta_t is not None and delta_t <= 1:
        return "Foggy", "foggy"

    # --- Wind ---
    if wind_avg >= 10:
        return "Windy", "windy"

    # --- Sky condition (daytime brightness only) ---
    if is_day:
        if brightness >= 50000 and uv >= 3:
            return "Sunny",         "clear-day"
        if brightness >= 30000:
            return "Clear",         "clear-day"
        if brightness >= 5000:
            return "Partly Cloudy", f"partly-cloudy-{tod}"
        return "Cloudy",            "cloudy"

    # Night: cloud cover not derivable from sensors
    return "Clear", "clear-night"
