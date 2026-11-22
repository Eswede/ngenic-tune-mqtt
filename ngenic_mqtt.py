#!/usr/bin/env python3
"""
ngenic_mqtt.py — Ngenic Tune API v3 -> MQTT med Home Assistant auto-discovery.

Ersatter den arkiverade HACS-integrationen (sfalkman/ngenic-hass-platform,
arkiverad mars 2025): en fristaende poller utan beroenden av doda repos.

Inga hemligheter i koden: all konfig via miljvariabler.

Kor som Docker Compose-tjanst (Dockerfile + docker-compose.yml +
ngenic.env-mall finns i canvasen "Ngenic-mqtt i Docker Compose":

  docker-compose.yml:
    services:
      ngenic-mqtt:
        build: .
        container_name: ngenic-mqtt
        restart: unless-stopped
        env_file: ngenic.env
        healthcheck:  (testar /data/heartbeat, skrivs efter varje poll)

  Dockerfile:
    FROM python:3.13-slim
    RUN pip install --no-cache-dir "paho-mqtt>=2.1,<3" "requests>=2.31,<3"
    COPY ngenic_mqtt.py .
    USER ngenic
    ENV HEARTBEAT_FILE=/data/heartbeat API_MIN_INTERVAL=1
    CMD ["python", "-u", "ngenic_mqtt.py"]

  ngenic.env (chmod 600):
    NGENIC_TOKEN=...        # personlig token fran developer.ngenic.se
    MQTT_HOST=192.168.1.x   # din MQTT-broker (LAN-IP, inte localhost)
    MQTT_PORT=1883
    MQTT_USER=...           # valfritt
    MQTT_PASS=...           # valfritt
)

Skapar per nod och mättyp sensor.ngenic_<nod>_<typ>. Verifierade riktiga
mättyper (live-test mot riktiga API:t nov 2026):

  controller_control_value_c    = den FEJKADE utetemperaturen Tune matar
                                  Siemens-styrningen med
  controller_temperature_c     = VERKLIG utetemperatur (controllerns utegivare)
                                  => "Paverkan" = control_value_C - temperature_C
  controller_process_value_c   = processvärdet (innetempen som styrs mot)
  controller_setpoint_value_c  = beräknad setpoint
  <rum>_temperature_c           = innegivare
  <rum>_target_temperature_c   = rummets maltemperatur
  <rum>_humidity_relative_pct  = luftfuktighet

samt:

  binary_sensor.ngenic_spot_price_active    elprisstyrning pa/av
  binary_sensor.ngenic_user_defined_controls egna styrvarden pa/av
  binary_sensor.ngenic_<nod>_connected      nodens anslutningsstatus
  sensor.ngenic_<nod>_battery               batteri i procent (om noden har batteri)
  sensor.ngenic_<nod>_radio                 signalniva 0..maxRadioStatus (diagnostik)
  binary_sensor.ngenic_schedule_active     semesterschema aktivt (= Tunes bortalage)
  sensor.ngenic_schedule_lowest_setpoint   schemats lagsta innetemp
  sensor.ngenic_last_poll                   heartbeat (timestamp; star den stilla
                                             > 10 min har pollern dott)

Alla svarformat har verifierats mot den officiella v3-specen och riktiga
API-svar (developer.ngenic.se): /latest svarar {"hasValue": true, "value": ...}
(hasValue=false hoppas over), /types svarar en ren lista med typstrangar,
/rooms har nodeUuid + targetTemperature, /nodestatus har
battery/maxBattery/isConnected/radioStatus, /setpointschedules har
startTime/endTime/lowestSetpoint/autoTune, /controlsettings har aven
controlOnUserDefinedControls.

API-referens: https://developer.ngenic.se  (noderna rapporterar var 5:e minut,
rate limit per minut OCH per timme -> hall POLL_INTERVAL >= 300 s; vill du
halvera antalet anrop kan du satta API_MIN_INTERVAL=1)
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
import paho.mqtt.client as mqtt

API = "https://app.ngenic.se/api/v3"

log = logging.getLogger("ngenic-mqtt")


def env(name, default=None, required=False):
    v = os.environ.get(name) or default
    if required and not v:
        log.error("Miljövariabel %s saknas — avbryter.", name)
        sys.exit(1)
    return v


TOKEN = env("NGENIC_TOKEN", required=True)
MQTT_HOST = env("MQTT_HOST", "localhost")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
MQTT_USER = env("MQTT_USER")
MQTT_PASS = env("MQTT_PASS")
POLL_INTERVAL = int(env("POLL_INTERVAL", "300"))
API_MIN_INTERVAL = float(env("API_MIN_INTERVAL", "0"))  # strypning: sek mellan API-anrop
HEARTBEAT_FILE = env("HEARTBEAT_FILE", "")  # skrivs efter varje lyckad poll (Docker healthcheck)
DISCOVERY_PREFIX = env("DISCOVERY_PREFIX", "homeassistant")
MQTT_CLIENT_ID = env("MQTT_CLIENT_ID", "ngenic-mqtt-poller")
AVAIL_TOPIC = f"ngenic/{MQTT_CLIENT_ID}/availability"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

# --- Ngenic API ---------------------------------------------------------------

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
})


def api_get(path):
    """GET med tålamod: 429-respekt, retry, None vid 404."""
    if API_MIN_INTERVAL:
        time.sleep(API_MIN_INTERVAL)
    for _ in range(3):
        try:
            r = session.get(API + path, timeout=30)
        except requests.RequestException as exc:
            log.warning("Nätverksfel mot %s: %s", path, exc)
            time.sleep(30)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After", "60")
            try:
                wait = min(600, max(5, float(retry_after)))
            except ValueError:
                wait = 60
            log.warning("Rate limited (429), vilar %.0f s", wait)
            time.sleep(wait)
            continue
        if r.status_code in (401, 403):
            log.error("Token avvisad (%s) — kontrollera NGENIC_TOKEN.", r.status_code)
            return None
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return None
    return None


# --- MQTT ---------------------------------------------------------------------

def make_client():
    try:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    except (AttributeError, TypeError):
        c = mqtt.Client(client_id=MQTT_CLIENT_ID)
    if MQTT_USER:
        c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.reconnect_delay_set(min_delay=1, max_delay=120)
    c.will_set(AVAIL_TOPIC, "offline", qos=1, retain=True)
    return c


client = make_client()


def on_connect(user_client, userdata, flags, rc, properties=None):
    log.info("MQTT ansluten (rc=%s)", rc)
    user_client.publish(AVAIL_TOPIC, "online", qos=1, retain=True)


client.on_connect = on_connect


def pub(topic, payload, retain=True):
    try:
        info = client.publish(topic, payload, qos=1, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS and info.rc != 0:
            log.warning("MQTT publicering misslyckades: %s (rc=%s)", topic, info.rc)
    except Exception as exc:
        log.warning("MQTT-fel på %s: %s", topic, exc)


# --- Hjälpfunktioner ----------------------------------------------------------

def slugify(name):
    return re.sub(r"[^a-z0-9_]+", "_", str(name).strip().lower()).strip("_") or "x"


# Verifierade mättyper (riktiga API-svar nov 2026 + officiell v3-spec)
UNIT_BY_TYPE = {
    "temperature_C": ("temperature", "°C"),
    "control_temp_C": ("temperature", "°C"),      # äldre namn i specexemplen
    "control_value_C": ("temperature", "°C"),    # FEJKADE utetemperaturen till styrningen
    "process_value_C": ("temperature", "°C"),    # processvärdet (innetemp)
    "setpoint_value_C": ("temperature", "°C"),   # beräknad setpoint
    "target_temperature_C": ("temperature", "°C"),
    "humidity_relative_percent": ("humidity", "%"),
    "humidity": ("humidity", "%"),                # äldre namn i specexemplen
    "power_W": ("power", "W"),
    "energy_Wh": ("energy", "Wh"),
}

# nodtyp -> etikett (delvis verifierat: 0=sensor, 1=controller, 2=gateway, 3=okänd)
NODE_TYPE_LABELS = {0: "Sensor", 1: "Controller", 2: "Gateway"}

# Vänliga entity-namn per (nodtyp, mätartyp). unique_id och state-topics lämnas
# orörda, så befintliga HA-entiteter bara byter visningsnamn (historiken behålls).
FRIENDLY_BY_TYPE = {
    ("Controller", "temperature_C"): "Outdoor (real)",
    ("Controller", "control_value_C"): "Outdoor (injected by Tune)",
    ("Controller", "control_temp_C"): "Outdoor (injected by Tune)",
    ("Controller", "process_value_C"): "Indoor (process value)",
    ("Controller", "setpoint_value_C"): "Setpoint",
    ("Sensor", "temperature_C"): "Indoor temperature",
    ("Sensor", "target_temperature_C"): "Target temperature",
    ("Sensor", "humidity_relative_percent"): "Humidity",
}
FRIENDLY_FALLBACK = {
    "temperature_C": "Temperature",
    "target_temperature_C": "Target temperature",
    "humidity_relative_percent": "Humidity",
    "humidity": "Humidity",
    "power_W": "Power",
    "energy_Wh": "Energy",
}


def friendly_label(node_type, mtype):
    nt = NODE_TYPE_LABELS.get(node_type, "node")
    return (FRIENDLY_BY_TYPE.get((nt, mtype))
            or FRIENDLY_FALLBACK.get(mtype, mtype))


def flatten_nodes(nodes, out):
    for node in nodes or []:
        out.append(node)
        flatten_nodes(node.get("children"), out)


def collect_uuids(obj, out):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower().endswith("uuid") and isinstance(value, str):
                out.add(value)
            else:
                collect_uuids(value, out)
    elif isinstance(obj, list):
        for item in obj:
            collect_uuids(item, out)


def extract_types(payload):
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, list):
        types = []
        for item in payload:
            if isinstance(item, str):
                types.append(item)
            elif isinstance(item, dict):
                t = item.get("type") or item.get("typeStr") or item.get("name")
                if t:
                    types.append(t)
        return types
    return []


def extract_value(payload):
    # officiellt svar: {"hasValue": true/false, "time": ..., "value": ...}
    def from_dict(d):
        if d.get("hasValue") is False:
            return None, None
        return d.get("value"), d.get("time")
    if isinstance(payload, dict) and "value" in payload:
        return from_dict(payload)
    if isinstance(payload, list) and payload:
        last = payload[-1]
        if isinstance(last, dict):
            return from_dict(last)
    return None, None


def discover(node_slug, node_uuid, mtype, node_name, label=None, device=None):
    device_class, unit = UNIT_BY_TYPE.get(mtype, (None, None))
    unique_id = f"ngenic_{node_slug}_{slugify(mtype)}"
    config = {
        "name": f"Ngenic {node_name} {label or mtype}",
        "unique_id": unique_id,
        "state_topic": f"ngenic/{node_slug}/{mtype}",
        "availability_topic": AVAIL_TOPIC,
        "device": device or {
            "identifiers": [f"ngenic_{node_uuid}"],
            "name": f"Ngenic {node_name}",
            "manufacturer": "Ngenic",
        },
    }
    if device_class:
        config["device_class"] = device_class
    if unit:
        config["unit_of_measurement"] = unit
    topic = f"{DISCOVERY_PREFIX}/sensor/{unique_id}/config"
    pub(topic, json.dumps(config), retain=True)
    return config["state_topic"]


# --- Pollning -----------------------------------------------------------------

def disc(kind, unique_id, name, state_topic, device_ids, device_name,
         device_class=None, unit=None, entity_category=None, payload_on=None):
    """Allmän HA-discovery för sensor/binary_sensor."""
    config = {
        "name": name,
        "unique_id": unique_id,
        "state_topic": state_topic,
        "availability_topic": AVAIL_TOPIC,
        "device": {"identifiers": device_ids, "name": device_name,
                   "manufacturer": "Ngenic"},
    }
    if device_class:
        config["device_class"] = device_class
    if unit:
        config["unit_of_measurement"] = unit
    if entity_category:
        config["entity_category"] = entity_category
    if payload_on is not None:
        config["payload_on"] = payload_on
        config["payload_off"] = "False" if payload_on == "True" else "off"
    pub(f"{DISCOVERY_PREFIX}/{kind}/{unique_id}/config", json.dumps(config), retain=True)


def parse_utc(ts):
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def poll_once():
    tunes = api_get("/tunes")
    if not isinstance(tunes, list) or not tunes:
        log.warning("Inga tunes hittades (eller token utan behörighet).")
        return
    bridge_dev = [MQTT_CLIENT_ID], "Ngenic bridge"

    for tune in tunes:
        tune_uuid = tune.get("tuneUuid") or tune.get("uuid")
        if not tune_uuid:
            continue
        # Alla entiteter publiceras pa EN gemensam HA-enhet per tune.
        main_dev = [f"ngenic_tune_{tune_uuid}"], "Ngenic Tune"
        bridge_dev = main_dev
        detail = api_get(f"/tunes/{tune_uuid}") or {}
        tune_name = detail.get("name") or tune.get("tuneName") or "tune"

        # rum -> namn per sensor-nod (+ rummens måltemperatur)
        room_names = {}
        for room in api_get(f"/tunes/{tune_uuid}/rooms") or []:
            room_name = room.get("name")
            uuids = set()
            collect_uuids(room, uuids)
            for u in uuids:
                room_names[u] = room_name
            target = room.get("targetTemperature")
            if room_name and isinstance(target, (int, float)):
                rslug = slugify(room_name)
                disc("sensor", f"ngenic_{rslug}_target_c",
                     f"Ngenic {room_name} target temperature",
                     f"ngenic/{rslug}/target_c",
                     main_dev[0], main_dev[1],
                     device_class="temperature", unit="°C")
                pub(f"ngenic/{rslug}/target_c", str(round(float(target), 1)),
                    retain=True)

        nodes = []
        flatten_nodes(api_get(f"/tunes/{tune_uuid}/gateway/nodes") or [], nodes)
        if not nodes and detail.get("gateway"):
            flatten_nodes([detail["gateway"]], nodes)
        # API:t kan returnera samma nod bade nested och pa toppniva: avdublettera
        _seen, nodes = set(), []
        _all = []
        flatten_nodes(api_get(f"/tunes/{tune_uuid}/gateway/nodes") or [], _all)
        if not _all and detail.get("gateway"):
            flatten_nodes([detail["gateway"]], _all)
        for n in _all:
            nu = n.get("uuid")
            if nu and nu not in _seen:
                _seen.add(nu)
                nodes.append(n)

        labels = {}
        for node in nodes:
            node_uuid = node.get("uuid")
            if not node_uuid:
                continue
            default_name = NODE_TYPE_LABELS.get(node.get("type"), f"node{node.get('type', '?')}")
            node_name = room_names.get(node_uuid, default_name)
            node_slug = slugify(f"{node_name}_{node_uuid[:8]}")
            labels[node_uuid] = (node_name, node_slug)

            ctrl_vals = {}  # controller: verklig + fejkad utetemp för influence
            mtypes = extract_types(
                api_get(f"/tunes/{tune_uuid}/measurements/{node_uuid}/types")
            )
            for mtype in mtypes:
                payload = api_get(
                    f"/tunes/{tune_uuid}/measurements/{node_uuid}/latest"
                    f"?type={mtype}"
                )
                value, _ts = extract_value(payload)
                if value is None:
                    continue
                state_topic = discover(node_slug, node_uuid, mtype, node_name,
                                        label=friendly_label(node.get("type"), mtype),
                                        device={"identifiers": main_dev[0],
                                                "name": main_dev[1],
                                                "manufacturer": "Ngenic"})
                if isinstance(value, float):
                    value = round(value, 2)
                pub(state_topic, str(value), retain=True)
                if node.get("type") == 1:
                    ctrl_vals[mtype] = value
                log.debug("%s/%s = %s", node_name, mtype, value)

            # Tune-inflytande ("influence"/Påverkan): fejkad - verklig utetemp,
            # publiceras direkt av bron på controller-enheten.
            if node.get("type") == 1:
                real = ctrl_vals.get("temperature_C")
                injected = ctrl_vals.get(
                    "control_value_C", ctrl_vals.get("control_temp_C")
                )
                if isinstance(real, (int, float)) and isinstance(injected, (int, float)):
                    disc("sensor", f"ngenic_{node_slug}_influence",
                         f"Ngenic Influence ({node_name})",
                         f"ngenic/{node_slug}/influence",
                         main_dev[0], main_dev[1],
                         device_class="temperature", unit="°C")
                    pub(f"ngenic/{node_slug}/influence",
                        str(round(injected - real, 2)), retain=True)
                    log.debug("%s/influence = %.2f", node_name, injected - real)

        # styrinställningar: elprisstyrning på/av
        settings = api_get(f"/tunes/{tune_uuid}/controlsettings") or {}
        if "controlOnSpotPrice" in settings:
            uid = "ngenic_spot_price_active"
            pub(
                f"{DISCOVERY_PREFIX}/binary_sensor/{uid}/config",
                json.dumps({
                    "name": "Ngenic spot price control active",
                    "unique_id": uid,
                    "state_topic": f"ngenic/{slugify(tune_name)}/spot_price_active",
                    "payload_on": "True",
                    "payload_off": "False",
                    "availability_topic": AVAIL_TOPIC,
                    "device": {"identifiers": main_dev[0],
                               "name": main_dev[1], "manufacturer": "Ngenic"},
                }),
                retain=True,
            )
            pub(
                f"ngenic/{slugify(tune_name)}/spot_price_active",
                str(settings.get("controlOnSpotPrice")),
                retain=True,
            )

        # extra fält i riktiga svaret: användardefinierade styrvärden på/av
        if "controlOnUserDefinedControls" in settings:
            uid = "ngenic_user_defined_controls"
            disc("binary_sensor", uid,
                 "Ngenic user-defined controls active",
                 f"ngenic/{slugify(tune_name)}/user_defined_controls",
                 main_dev[0], main_dev[1], payload_on="True")
            pub(
                f"ngenic/{slugify(tune_name)}/user_defined_controls",
                str(settings.get("controlOnUserDefinedControls")),
                retain=True,
            )

        # nodstatus: batteri, radio, ansluten (GET /nodestatus)
        for st in api_get(f"/tunes/{tune_uuid}/nodestatus") or []:
            nu = st.get("nodeUuid")
            if not nu:
                continue
            nname, nslug = labels.get(nu, (f"node{nu[:8]}", slugify(f"node_{nu[:8]}")))
            dev = main_dev
            disc("binary_sensor", f"ngenic_{nslug}_connected",
                 f"Ngenic {nname} connected", f"ngenic/{nslug}/connected",
                 dev[0], dev[1], payload_on="True")
            pub(f"ngenic/{nslug}/connected", str(st.get("isConnected")), retain=True)
            maxb, b = st.get("maxBattery"), st.get("battery")
            if isinstance(maxb, int) and maxb > 0 and isinstance(b, int):
                disc("sensor", f"ngenic_{nslug}_battery",
                     f"Ngenic {nname} battery", f"ngenic/{nslug}/battery",
                     dev[0], dev[1], device_class="battery", unit="%")
                pub(f"ngenic/{nslug}/battery", str(round(100 * b / maxb)), retain=True)
            # radioStatus är en diagnostisk niv 0..maxRadioStatus
            disc("sensor", f"ngenic_{nslug}_radio",
                 f"Ngenic {nname} radio signal", f"ngenic/{nslug}/radio",
                 dev[0], dev[1], entity_category="diagnostic")
            pub(f"ngenic/{nslug}/radio", str(st.get("radioStatus")), retain=True)

        # semesterscheman (GET /setpointschedules): aktivt schema + lägsta temp
        schedules = api_get(f"/tunes/{tune_uuid}/setpointschedules") or []
        now = datetime.now(timezone.utc)
        active = None
        for sch in schedules:
            start = parse_utc(sch.get("startTime"))
            end = parse_utc(sch.get("endTime"))
            if start and end and start <= now <= end:
                active = sch
                break
        tslug = slugify(tune_name)
        tune_dev = main_dev
        disc("binary_sensor", "ngenic_schedule_active",
             "Ngenic away schedule active", f"ngenic/{tslug}/schedule_active",
             tune_dev[0], tune_dev[1], payload_on="True")
        pub(f"ngenic/{tslug}/schedule_active", str(active is not None), retain=True)
        disc("sensor", "ngenic_schedule_lowest_setpoint",
             "Ngenic away schedule lowest temp", f"ngenic/{tslug}/schedule_lowest",
             tune_dev[0], tune_dev[1], device_class="temperature", unit="°C")
        disc("sensor", "ngenic_schedule_name", "Ngenic away schedule name",
             f"ngenic/{tslug}/schedule_name", tune_dev[0], tune_dev[1])
        if active:
            pub(f"ngenic/{tslug}/schedule_lowest",
                str(round(float(active.get("lowestSetpoint")), 1)), retain=True)
            pub(f"ngenic/{tslug}/schedule_name",
                str(active.get("name", "")), retain=True)
        else:
            pub(f"ngenic/{tslug}/schedule_lowest", "", retain=True)
            pub(f"ngenic/{tslug}/schedule_name", "", retain=True)

        log.info("Pollad tune '%s': %d noder, %d scheman", tune_name, len(nodes), len(schedules))

    # heartbeat
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    uid = "ngenic_last_poll"
    pub(
        f"{DISCOVERY_PREFIX}/sensor/{uid}/config",
        json.dumps({
            "name": "Ngenic last poll",
            "unique_id": uid,
            "state_topic": f"ngenic/{MQTT_CLIENT_ID}/last_poll",
            "device_class": "timestamp",
            "availability_topic": AVAIL_TOPIC,
            "entity_category": "diagnostic",
            "device": {"identifiers": bridge_dev[0],
                       "name": bridge_dev[1], "manufacturer": "Ngenic"},
        }),
        retain=True,
    )
    pub(f"ngenic/{MQTT_CLIENT_ID}/last_poll", now, retain=True)


def main():
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as exc:
            log.warning("MQTT-brokern går inte att nå (%s), försöker igen om 15 s.", exc)
            time.sleep(15)
    client.loop_start()

    log.info("Startar poller var %d s mot %s", POLL_INTERVAL, MQTT_HOST)
    while True:
        try:
            poll_once()
            if HEARTBEAT_FILE:
                try:
                    with open(HEARTBEAT_FILE, "w", encoding="utf-8") as fh:
                        fh.write(str(time.time()))
                except OSError as exc:
                    log.warning("Kan ej skriva heartbeat %s: %s", HEARTBEAT_FILE, exc)
        except requests.HTTPError as exc:
            log.error("API-fel: %s", exc)
        except Exception:
            log.exception("Oväntat fel i pollningen")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
