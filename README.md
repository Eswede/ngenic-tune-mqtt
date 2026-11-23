# ngenic-tune-mqtt

Standalone [Ngenic Tune](https://ngenic.se) → MQTT bridge with Home Assistant
auto-discovery, in a single Python file. Polls the official Ngenic Tune API (v3)
and publishes every measurement, node status, room setpoint and vacation
schedule over MQTT. No cloud tools, no archived HACS integration, no broker
plugins needed.

This fills the gap left by the popular but now archived
[ngenic-hass-platform](https://github.com/sfalkman/ngenic-hass-platform)
integration (removed from HACS in March 2025: it has not been maintained since
2021 and its repo is read-only).

## What you get in Home Assistant

Sensors are created automatically via MQTT discovery (`sensor.ngenic_*`).
All entities are grouped onto a single `Ngenic Tune` device per Tune, instead of
one device per radio node:

| Entity | Meaning |
|---|---|
| `ngenic_<room>_temperature_c` | Indoor sensor temperature |
| `ngenic_<room>_humidity_relative_percent` | Humidity (if the sensor reports it) |
| `ngenic_<room>_target_temperature_c` / `..._target_c` | Room target temperature |
| `ngenic_controller_<id>_temperature_c` | **Outdoor temperature at the controller's own sensor**|
| `ngenic_controller_<id>_control_value_c` | **Fake outdoor temperature Tune feeds your heat system** |
| `ngenic_controller_<id>_process_value_c` / `..._setpoint_value_c` | Process value / computed setpoint |
| **`ngenic_controller_<id>_influence`** | **Tune's "Påverkan" (influence), computed by the bridge: `control_value_C − temperature_C`** |
| `binary_sensor.ngenic_<node>_connected` | Node connectivity |
| `sensor.ngenic_<node>_battery` | Battery % (battery-powered nodes) |
| `sensor.ngenic_<node>_radio` | Radio signal level (diagnostic) |
| `binary_sensor.ngenic_schedule_active` | Vacation/away schedule active |
| `sensor.ngenic_schedule_lowest_setpoint` | Lowest indoor temp during schedule |
| `binary_sensor.ngenic_spot_price_active` / `..._user_defined_controls` | Control settings flags |
| `sensor.ngenic_last_poll` | Heartbeat timestamp |

### The interesting part: Tune's "influence" (Påverkan)

The controller node reports both the outdoor temperature at its own sensor
(`temperature_C`) and the fictional one it feeds your heating system
(`control_value_C`). Their difference is exactly the offset Tune adds, which
the Ngenic app calls *Påverkan*. The bridge computes and publishes it directly
as the sensor **`Ngenic Influence (<controller name>)`** — no extra setup
needed. With several controllers, each one gets its own influence sensor.

> **Note:** `temperature_C` is measured by the controller node itself. If it is
> mounted in a cold utility room it can sit a few degrees off the true outdoor
> temperature. Influence is still exact, since both terms come from the same
> node — but do not use it to compare against a weather station.

Since heating curves slope down, a **positive** influence means Tune pretends
it is warmer outside than it is, which **lowers** the forward temperature: the
energy-saving direction. Ngenic's rule of thumb: if it stays beyond ±4 °C on
average through the heating season, your heating curve is badly set.

## Quick start (Docker Compose)

```bash
git clone https://github.com/Eswede/ngenic-tune-mqtt.git
cd ngenic-tune-mqtt
cp ngenic.env.example ngenic.env   # add your token + broker, then:
chmod 600 ngenic.env
docker compose up -d --build
```

The container runs as a non-root user, restarts automatically and has a
healthcheck based on a heartbeat file written after each successful poll.

**Token**: create a personal access token at
[developer.ngenic.se](https://developer.ngenic.se) (Get access token) and put
it in `ngenic.env`. It never ends up in the image or in git.

## Configuration (environment variables)

| Variable | Default | Notes |
|---|---|---|
| `NGENIC_TOKEN` | required | Personal access token |
| `MQTT_HOST` | `localhost` | Broker address. Not `localhost` unless the broker shares the container network; use the LAN IP or a compose service name |
| `MQTT_PORT` | `1883` | |
| `MQTT_USER` / `MQTT_PASS` | empty | Optional |
| `POLL_INTERVAL` | `300` | Nodes report every 5 minutes; polling faster is pointless |
| `API_MIN_INTERVAL` | `1` (Docker) | Seconds between API calls. The API rate-limits per minute **and** per hour; the bridge respects 429 + `Retry-After` |
| `HEARTBEAT_FILE` | `/data/heartbeat` | Written after each successful poll, used by the compose healthcheck |
| `MQTT_CLIENT_ID` | `ngenic-mqtt-poller` | |
| `DISCOVERY_PREFIX` | `homeassistant` | |

## Preview without a broker

Run the full bridge against the real API and print every MQTT message instead
of publishing:

```bash
pip install requests
NGENIC_TOKEN=your-token python tests/preview.py
```

## Verified against the real API

Response shapes (list/`/types`, `hasValue` handling in `/latest`, room
`nodeUuid`, duplicate node listings in `/gateway/nodes`, battery/radio in
`/nodestatus`, vacation schedules in `/setpointschedules`) were verified
against the official v3 specification and live API responses, including
429 rate-limit handling, in November 2026.

## Notes and disclaimers

- Not affiliated with Ngenic. Ngenic Tune requires an active Ngenic
  subscription; the API can change or disappear at any time.
- Importing a heat curve offset in the style of this bridge is exactly what the
  commercial product does to your heat system; this repository only *reads*
  data.
- Requires Python 3.9+ (`paho-mqtt>=2.1`, `requests`).

## License

[MIT](LICENSE)
