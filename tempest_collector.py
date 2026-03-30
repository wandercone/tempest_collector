import hashlib
import json
import logging
import sys
import time
from datetime import datetime
import uuid
from pathlib import Path

import requests
from dotenv import load_dotenv

from utils.conditions import degrees_to_cardinal, derive_conditions_icon
from utils.config import require_env
from utils.db import get_db_connection
from utils.log import configure_logging
from utils.notify import send_gotify

load_dotenv(Path(__file__).parent / ".env")

env = require_env(
    [
        "API_KEY",
        "STATION_ID",
        "OBS_TABLE",
        "HOURLY_TABLE",
        "DAILY_TABLE",
        "GOTIFY_URL",
        "GOTIFY_TOKEN",
    ]
)

STATION_ID = int(env["STATION_ID"])
API_URL = f"https://swd.weatherflow.com/swd/rest/observations/station/{STATION_ID}"
FORECAST_URL = "https://swd.weatherflow.com/swd/rest/better_forecast"
BUFFER_FILE = Path("/var/tmp/weather_buffer.jsonl")
STALE_FLAG = Path("/var/tmp/weather_station_offline")

STALE_THRESHOLD = 180  # seconds - observation older than this means station is offline
BUFFER_WARN_EVERY = (
    10  # send a Gotify alert every N buffered observations during a DB outage
)

KNOWN_COLUMNS = {
    "station_id",
    "timestamp",
    "conditions",
    "icon",
    "wind_direction_cardinal",
    "air_temperature",
    "relative_humidity",
    "dew_point",
    "wet_bulb_temperature",
    "wet_bulb_globe_temperature",
    "delta_t",
    "feels_like",
    "heat_index",
    "wind_chill",
    "barometric_pressure",
    "station_pressure",
    "sea_level_pressure",
    "pressure_trend",
    "wind_avg",
    "wind_gust",
    "wind_lull",
    "wind_direction",
    "precip",
    "precip_accum_last_1hr",
    "precip_accum_local_day",
    "precip_accum_local_day_final",
    "precip_accum_local_yesterday",
    "precip_accum_local_yesterday_final",
    "precip_analysis_type_yesterday",
    "precip_minutes_local_day",
    "precip_minutes_local_yesterday",
    "precip_minutes_local_yesterday_final",
    "solar_radiation",
    "uv",
    "brightness",
    "lightning_strike_count",
    "lightning_strike_count_last_1hr",
    "lightning_strike_count_last_3hr",
    "lightning_strike_last_distance",
    "air_density",
}


def notify(title: str, message: str, priority: int) -> None:
    """Send a Gotify notification using credentials from the environment."""
    send_gotify(
        title=title,
        message=message,
        priority=priority,
        gotify_url=env["GOTIFY_URL"],
        app_token=env["GOTIFY_TOKEN"],
    )


def make_id(station_id: int, timestamp: int) -> str:
    """Return a deterministic UUID derived from station_id and timestamp via SHA-256."""
    data = f"{station_id}:{timestamp}".encode()
    digest = hashlib.sha256(data).digest()[:16]
    return str(uuid.UUID(bytes=digest))


def make_forecast_id(station_id: int, fetched_at: int, forecast_time: int) -> str:
    """Return a deterministic UUID derived from station_id, fetch time, and forecast time via SHA-256."""
    data = f"{station_id}:{fetched_at}:{forecast_time}".encode()
    digest = hashlib.sha256(data).digest()[:16]
    return str(uuid.UUID(bytes=digest))


def fetch_observation(api_key: str) -> dict | None:
    """Fetch the latest observation from the Tempest station API. Returns the first obs dict or None."""
    resp = requests.get(API_URL, params={"token": api_key}, timeout=10)
    resp.raise_for_status()
    obs_list = resp.json().get("obs")
    return obs_list[0] if obs_list else None


def fetch_forecast(api_key: str) -> dict | None:
    """Fetch the better_forecast payload for the station. Returns the full JSON dict or None."""
    resp = requests.get(
        FORECAST_URL, params={"station_id": str(STATION_ID), "token": api_key}, timeout=10
    )
    resp.raise_for_status()
    return resp.json()


def insert_forecast_hourly(
    conn, rows: list, fetched_at: int, log: logging.Logger
) -> None:
    """Insert hourly forecast rows into the hourly table, skipping duplicates via INSERT IGNORE."""
    for row in rows:
        forecast_time = row.get("time")
        if forecast_time is None:
            continue
        row_id = make_forecast_id(STATION_ID, fetched_at, forecast_time)
        data = {
            "station_id": STATION_ID,
            "fetched_at": fetched_at,
            "forecast_time": forecast_time,
            "conditions": row.get("conditions"),
            "icon": row.get("icon"),
            "air_temperature": row.get("air_temperature"),
            "feels_like": row.get("feels_like"),
            "wind_avg": row.get("wind_avg"),
            "wind_direction": row.get("wind_direction"),
            "wind_direction_cardinal": row.get("wind_direction_cardinal"),
            "precip_probability": row.get("precip_probability"),
            "precip": row.get("precip"),
            "precip_type": row.get("precip_type"),
            "relative_humidity": row.get("relative_humidity"),
            "uv": row.get("uv"),
        }
        columns = "`id`, " + ", ".join(f"`{c}`" for c in data)
        placeholders = ", ".join(["%s"] * (len(data) + 1))
        sql = f"INSERT IGNORE INTO `{env['HOURLY_TABLE']}` ({columns}) VALUES ({placeholders})"
        with conn.cursor() as cursor:
            cursor.execute(sql, [row_id] + list(data.values()))
    conn.commit()
    log.debug("Inserted %d hourly forecast rows", len(rows))


def insert_forecast_daily(
    conn, rows: list, fetched_at: int, log: logging.Logger
) -> None:
    """Insert daily forecast rows into the daily table, skipping duplicates via INSERT IGNORE."""
    for row in rows:
        day_start = row.get("day_start_local")
        if day_start is None:
            continue
        row_id = make_forecast_id(STATION_ID, fetched_at, day_start)
        data = {
            "station_id": STATION_ID,
            "fetched_at": fetched_at,
            "day_start_local": day_start,
            "conditions": row.get("conditions"),
            "icon": row.get("icon"),
            "air_temp_high": row.get("air_temp_high"),
            "air_temp_low": row.get("air_temp_low"),
            "precip_probability": row.get("precip_probability"),
            "precip_type": row.get("precip_type"),
            "sunrise": row.get("sunrise"),
            "sunset": row.get("sunset"),
        }
        columns = "`id`, " + ", ".join(f"`{c}`" for c in data)
        placeholders = ", ".join(["%s"] * (len(data) + 1))
        sql = f"INSERT IGNORE INTO `{env['DAILY_TABLE']}` ({columns}) VALUES ({placeholders})"
        with conn.cursor() as cursor:
            cursor.execute(sql, [row_id] + list(data.values()))
    conn.commit()
    log.debug("Inserted %d daily forecast rows", len(rows))


def check_staleness(obs: dict, log: logging.Logger) -> bool:
    """Returns True if the observation is stale (station offline). Sends Gotify on state change."""
    age = int(time.time()) - obs["timestamp"]
    if age > STALE_THRESHOLD:
        if not STALE_FLAG.exists():
            STALE_FLAG.touch()
            log.warning(
                "Station %s appears offline - last observation is %ds old",
                STATION_ID,
                age,
            )
            notify(
                title="Weather Station Offline",
                message=f"Station {STATION_ID} has not reported for {age // 60} minutes.",
                priority=7,
            )
        return True
    else:
        if STALE_FLAG.exists():
            STALE_FLAG.unlink()
            log.info("Station %s is back online", STATION_ID)
            notify(
                title="Weather Station Back Online",
                message=f"Station {STATION_ID} is reporting again.",
                priority=5,
            )
        return False

def buffer_observation(obs: dict) -> None:
    """Append a single observation dict as a JSON line to the buffer file."""
    with BUFFER_FILE.open("a") as f:
        f.write(json.dumps(obs) + "\n")

def insert_observation(conn, table: str, obs: dict) -> None:
    """Insert a single observation into the given table, filtering to KNOWN_COLUMNS only."""
    row = {k: v for k, v in obs.items() if k in KNOWN_COLUMNS}
    row_id = make_id(row["station_id"], row["timestamp"])
    columns = "`id`, " + ", ".join(f"`{c}`" for c in row)
    placeholders = ", ".join(["%s"] * (len(row) + 1))
    sql = f"INSERT IGNORE INTO `{table}` ({columns}) VALUES ({placeholders})"
    with conn.cursor() as cursor:
        cursor.execute(sql, [row_id] + list(row.values()))
    conn.commit()



def flush_buffer(conn, table: str, log: logging.Logger) -> None:
    """Replay all buffered observations into the DB oldest-first; removes the file when done.

    On insert failure, the unprocessed remainder is written back to the buffer and the
    exception is re-raised so the caller can exit cleanly.
    """
    if not BUFFER_FILE.exists():
        return

    lines = [line for line in BUFFER_FILE.read_text().splitlines() if line.strip()]
    if not lines:
        BUFFER_FILE.unlink()
        return

    log.info("Flushing %d buffered observation(s)", len(lines))

    for i, line in enumerate(lines):
        try:
            obs = json.loads(line)
        except json.JSONDecodeError:
            log.warning("Skipping malformed buffer entry at line %d", i + 1)
            continue

        try:
            insert_observation(conn, table, obs)
        except Exception as exc:
            log.error(
                "Flush failed at line %d: %s - leaving remainder in buffer", i + 1, exc
            )
            BUFFER_FILE.write_text("\n".join(lines[i:]) + "\n")
            raise

    BUFFER_FILE.unlink()
    log.info("Buffer flushed and cleared")

def main() -> None:
    """Fetch one observation, derive conditions, buffer it, then flush to DB.

    At the top of each hour also fetches the better_forecast payload to override
    conditions/icon with WeatherFlow's values and persist hourly/daily forecast rows.
    Exits 0 on expected DB unavailability (backup window); exits 1 on hard failures.
    """
    log = configure_logging()

    api_key = env["API_KEY"]
    obs_table = env["OBS_TABLE"]
    top_of_hour = datetime.now().minute == 0

    # Fetch current observation from API
    try:
        obs = fetch_observation(api_key)
    except Exception as exc:
        log.error("Failed to fetch observation: %s", exc)
        sys.exit(1)

    if not obs:
        log.warning("API returned no observation data")
        sys.exit(1)

    obs["station_id"] = STATION_ID

    # Check if the station is actually reporting fresh data
    check_staleness(obs, log)

    # Derive conditions/icon and cardinal direction from sensor data every minute
    if obs.get("wind_direction") is not None:
        obs["wind_direction_cardinal"] = degrees_to_cardinal(obs["wind_direction"])
    obs["conditions"], obs["icon"] = derive_conditions_icon(obs)

    # At the top of each hour, override with WeatherFlow's values (handles night cloud cover)
    forecast = None
    if top_of_hour:
        try:
            forecast = fetch_forecast(api_key)
            if forecast:
                cc = forecast.get("current_conditions", {})
                obs["conditions"] = cc.get("conditions") or obs["conditions"]
                obs["icon"] = cc.get("icon") or obs["icon"]
                obs["wind_direction_cardinal"] = (
                    cc.get("wind_direction_cardinal") or obs["wind_direction_cardinal"]
                )
                log.debug(
                    "Forecast fetched  conditions=%s icon=%s",
                    obs["conditions"],
                    obs["icon"],
                )
        except Exception as exc:
            log.warning("Failed to fetch forecast: %s - using derived values", exc)

    # Always write to buffer first - guarantees no data loss regardless of DB state
    buffer_observation(obs)
    log.debug("Observation buffered  timestamp=%s", obs.get("timestamp"))

    # Warn if the buffer is growing large (DB outage beyond expected window)
    if BUFFER_FILE.exists():
        queued = sum(1 for line in BUFFER_FILE.read_text().splitlines() if line.strip())
        if queued > 0 and queued % BUFFER_WARN_EVERY == 0:
            log.warning(
                "DB outage: %d observations queued (~%d minutes)", queued, queued
            )
            notify(
                title="Weather DB Outage",
                message=f"{queued} observations queued - database unreachable for ~{queued} minutes.",
                priority=7,
            )

    # Attempt DB connection
    try:
        conn = get_db_connection()
    except Exception as exc:
        log.warning("DB unavailable, observation queued for next run: %s", exc)
        sys.exit(0)  # expected during backup window - not a failure

    # Flush buffer oldest-first (includes the observation just written)
    try:
        flush_buffer(conn, obs_table, log)
    except Exception:
        conn.close()
        sys.exit(1)

    # At top of hour, insert forecast rows (not buffered - missed forecasts are acceptable)
    if top_of_hour and forecast:
        fetched_at = obs["timestamp"]
        hourly = forecast.get("forecast", {}).get("hourly", [])
        daily = forecast.get("forecast", {}).get("daily", [])
        try:
            insert_forecast_hourly(conn, hourly, fetched_at, log)
            insert_forecast_daily(conn, daily, fetched_at, log)
        except Exception as exc:
            log.error("Failed to insert forecast data: %s", exc)

    conn.close()



if __name__ == "__main__":
    main()
