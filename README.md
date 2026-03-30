# Weather Collection

Polls a [WeatherFlow Tempest](https://weatherflow.com/tempest-weather-system/) weather station once per minute via Tempest's API and stores observations in MariaDB. Runs as a systemd user service.

## Features

- Per-minute observation collection with derived conditions/icon and cardinal wind direction
- Hourly and daily forecast snapshots from the WeatherFlow `better_forecast` API
- Write-ahead buffer (`/var/tmp/weather_buffer.jsonl`) - observations survive DB outages and are replayed oldest-first on reconnect
- Gotify notifications on station offline/online state changes and extended DB outages
- Deterministic SHA-256 UUIDs as primary keys - safe to replay; `INSERT IGNORE` prevents duplicates

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```
API_KEY=your_weatherflow_api_token
STATION_ID=your_station_id
OBS_TABLE=weather_observations
HOURLY_TABLE=weather_forecast_hourly
DAILY_TABLE=weather_forecast_daily
GOTIFY_URL=https://your-gotify-host
GOTIFY_TOKEN=your_gotify_app_token
```

## Database setup

```
mysql -u root -p < create_table.sql
```

## Usage

Run manually:

```
python3 tempest_collector.py
```

Or install as a systemd user service:

```
cp tempest-collector.service tempest-collector.timer ~/.config/systemd/user/
Edit tempest-collector.service for saved pathing
systemctl --user daemon-reload
systemctl --user enable --now tempest-collector.timer
```