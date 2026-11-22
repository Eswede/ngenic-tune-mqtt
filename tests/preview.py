#!/usr/bin/env python3
"""Preview mode: run the full bridge against the real Ngenic API and
print every MQTT message it would publish, without needing an MQTT broker
(paho is stubbed by a recorder, requests by stdlib urllib).

Usage:  NGENIC_TOKEN=your-token python tests/preview.py
"""
import importlib.util
import io
import json
import os
import sys
import types

os.environ["API_MIN_INTERVAL"] = "0.8"          # strypning: vänlig mot rate limits
if not os.environ.get("NGENIC_TOKEN"):
    sys.exit("Set NGENIC_TOKEN to run the preview")

# ---- requests-stub på riktig urllib -------------------------------------------
requests_stub = types.ModuleType("requests")

class RequestException(Exception):
    pass

requests_stub.RequestException = RequestException

class UrllibResponse:
    def __init__(self, data, status):
        self.status_code = status
        self.headers = {"Retry-After": "0"}
        self._buf = io.BytesIO(data)

    def json(self):
        return json.loads(self._buf.getvalue())

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RequestException(f"HTTP {self.status_code}")

class UrllibSession:
    def __init__(self):
        self.headers = {}

    def get(self, url, timeout=30):
        import urllib.request, urllib.error
        req = urllib.request.Request(url, headers=self.headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return UrllibResponse(resp.read(), resp.status)
        except urllib.error.HTTPError as e:
            return UrllibResponse(e.read(), e.code)
        except urllib.error.URLError as e:
            raise RequestException(str(e))

requests_stub.Session = UrllibSession
sys.modules["requests"] = requests_stub

# ---- paho-stub ----------------------------------------------------------------
mqtt_pkg = types.ModuleType("paho")
mqtt_mod = types.ModuleType("paho.mqtt")
mqtt_client_mod = types.ModuleType("paho.mqtt.client")

class CallbackAPIVersion:
    VERSION2 = "VERSION2"

published = []

class RecClient:
    def __init__(self, *a, **k):
        self.on_connect = None
    def username_pw_set(self, *a): pass
    def reconnect_delay_set(self, *a, **k): pass
    def will_set(self, topic, payload, qos=0, retain=False):
        published.append({"topic": topic, "payload": payload, "kind": "will"})
    def connect(self, *a, **k): return 0
    def loop_start(self): pass
    def publish(self, topic, payload, qos=0, retain=False):
        published.append({"topic": topic, "payload": payload, "kind": "pub", "retain": retain})
        class Info: rc = 0
        return Info()

mqtt_client_mod.Client = RecClient
mqtt_client_mod.CallbackAPIVersion = CallbackAPIVersion
mqtt_client_mod.MQTT_ERR_SUCCESS = 0
sys.modules["paho"] = mqtt_pkg
sys.modules["paho.mqtt"] = mqtt_mod
sys.modules["paho.mqtt.client"] = mqtt_client_mod

# ---- kör ----------------------------------------------------------------------
spec = importlib.util.spec_from_file_location("ngenic_mqtt", "ngenic_mqtt.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.API_MIN_INTERVAL = 0.8

print("kör poll_once() mot riktiga API:t...\n")
mod.poll_once()

configs = [p for p in published if p["topic"].endswith("/config")]
states = [p for p in published if not p["topic"].endswith("/config") and p["kind"] == "pub"]
wills = [p for p in published if p["kind"] == "will"]
print(f"\nMeddelanden: {len(configs)} discovery + {len(states)} tillstånd + {len(wills)} LWT")
print(f"{'MQTT-topic':<62}{'kvarhållen':<12}värde/typ")
for s in sorted(states, key=lambda p: p["topic"]):
    print(f"{s['topic']:<62}{'ja' if s['retain'] else 'nej':<12}{s['payload']}")
print()
uids = [json.loads(c["payload"]).get("unique_id") for c in configs]
assert len(uids) == len(set(uids)), "unique_id-krock!"
bad = [c for c in configs if not c["retain"]]
print("validering:", "ALLT OK" if not bad else "FEL: ej retained discovery")
