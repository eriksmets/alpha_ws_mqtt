#!/usr/bin/env python3
import argparse
import html
import json
import logging
import re
import signal
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt
import websocket
import yaml


LOG = logging.getLogger("alpha_ws_mqtt")
STOP = threading.Event()


def slugify(value: str) -> str:
    value = str(value).strip().lower()
    value = re.sub(r"[^\w]+", "_", value, flags=re.UNICODE)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def strip_html(value: Any) -> str:
    text = str(value)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def parse_state(raw: Any) -> Dict[str, Any]:
    text = strip_html(raw).replace(",", ".").strip()

    if text in {"Aan", "Ein", "On", "ON", "true", "True", "TRUE"}:
        return {"state": "ON", "raw": text}

    if text in {"Uit", "Aus", "Off", "OFF", "false", "False", "FALSE"}:
        return {"state": "OFF", "raw": text}

    if text in {"", "---", "--- l/h", "--- l/min"}:
        return {"state": "unknown", "raw": text}

    if re.match(r"^\d{1,4}:\d{2}(:\d{2})?$", text):
        return {"state": text, "raw": text}

    m = re.match(r"^([-+]?\d+(?:\.\d+)?)\s*([^\d\s].*)?$", text)
    if m:
        num = float(m.group(1))
        if num.is_integer():
            num = int(num)
        return {"state": num, "raw": text}

    return {"state": text, "raw": text}


def message_type(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    return str(data.get("type") or "")


def path_suffix_matches(full_path: List[str], wanted_path: List[str]) -> bool:
    if not wanted_path:
        return True

    full = [norm(x) for x in full_path]
    wanted = [norm(x) for x in wanted_path]

    if len(wanted) > len(full):
        return False

    return full[-len(wanted):] == wanted


@dataclass
class ConfiguredValue:
    section: str
    section_path: List[str]
    name: str
    occurrence: int
    label: str
    display_name: str
    topic: str
    unit: str
    device_class: str
    state_class: str
    resolved_id: Optional[str] = None


@dataclass
class ContentEntry:
    node_id: str
    name: str
    section: str
    section_path: List[str]


class AlphaWsMqttBridge:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.alpha = config["alpha"]
        self.mqtt_config = config["mqtt"]

        self.base_topic = self.mqtt_config.get("base_topic", "alpha_innotec").strip("/")
        self.retain = bool(self.mqtt_config.get("retain", True))
        self.discovery_prefix = self.mqtt_config.get("discovery_prefix", "homeassistant").strip("/")

        self.values = self.load_values(config.get("values", []))

        self.nav_name_to_id: Dict[str, str] = {}
        self.latest_by_id: Dict[str, Any] = {}
        self.content_entries: List[ContentEntry] = []

        self.current_ws: Optional[websocket.WebSocket] = None
        self.mqtt = self.create_mqtt_client()

    def load_values(self, rows: List[Dict[str, Any]]) -> List[ConfiguredValue]:
        values: List[ConfiguredValue] = []

        for row in rows:
            label = slugify(row.get("label") or row.get("name") or "value")

            topic = row.get("topic")
            if not topic:
                topic = f"{self.base_topic}/{label}/state"

            section_path = row.get("section_path") or []
            if isinstance(section_path, str):
                section_path = [section_path]

            values.append(
                ConfiguredValue(
                    section=str(row.get("section") or ""),
                    section_path=[str(x) for x in section_path],
                    name=str(row.get("name") or ""),
                    occurrence=int(row.get("occurrence", 1)),
                    label=label,
                    display_name=str(row.get("display_name") or row.get("name") or label),
                    topic=str(topic).strip("/"),
                    unit=str(row.get("unit") or ""),
                    device_class=str(row.get("device_class") or ""),
                    state_class=str(row.get("state_class") or ""),
                )
            )

        return values

    def create_mqtt_client(self) -> mqtt.Client:
        client_id = self.mqtt_config.get("client_id", "alpha-innotec-ws-mqtt")

        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        except Exception:
            client = mqtt.Client(client_id=client_id)

        username = self.mqtt_config.get("username") or ""
        password = self.mqtt_config.get("password") or ""

        if username:
            client.username_pw_set(username, password)

        client.on_connect = self.on_mqtt_connect
        client.will_set(f"{self.base_topic}/status", "offline", retain=True)

        return client

    def on_mqtt_connect(self, client, userdata, flags, reason_code=None, properties=None):
        LOG.info("MQTT connected")
        client.publish(f"{self.base_topic}/status", "online", retain=True)

    def connect_mqtt(self):
        host = self.mqtt_config["host"]
        port = int(self.mqtt_config.get("port", 1883))

        LOG.info("Connecting MQTT %s:%s", host, port)
        self.mqtt.connect(host, port, keepalive=60)
        self.mqtt.loop_start()

    def disconnect_mqtt(self):
        try:
            self.mqtt.publish(f"{self.base_topic}/status", "offline", retain=True)
        except Exception:
            pass

        try:
            self.mqtt.loop_stop()
        except Exception:
            pass

        try:
            self.mqtt.disconnect()
        except Exception:
            pass

    def publish_discovery(self):
        """
        Publishes HA MQTT discovery ONLY for sensors listed under YAML values:.
        It does not publish discovery for any auto-discovered WebSocket id.
        """
        if not self.mqtt_config.get("discovery", True):
            return

        device = {
            "identifiers": ["alpha_innotec_luxtronik_ws"],
            "name": "Alpha Innotec",
            "manufacturer": "Alpha Innotec",
            "model": "Luxtronik WebSocket",
        }

        for item in self.values:
            object_id = f"alpha_innotec_{item.label}"
            config_topic = f"{self.discovery_prefix}/sensor/{object_id}/config"

            payload = {
                "name": item.display_name,
                "unique_id": object_id,
                "state_topic": item.topic,
                "availability_topic": f"{self.base_topic}/status",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device,
            }

            if item.unit:
                payload["unit_of_measurement"] = item.unit

            if item.device_class:
                payload["device_class"] = item.device_class

            if item.state_class:
                payload["state_class"] = item.state_class

            self.mqtt.publish(
                config_topic,
                json.dumps(payload, ensure_ascii=False),
                retain=True,
                qos=0,
            )

            LOG.info("HA discovery configured topic only: %s -> %s", config_topic, item.topic)

        LOG.info("HA discovery published for %s configured sensors only", len(self.values))

    def build_ws_url(self) -> str:
        scheme = self.alpha.get("scheme", "ws")
        host = self.alpha["host"]
        port = int(self.alpha.get("port", 8214))
        path = self.alpha.get("path", "") or ""

        if path and not path.startswith("/"):
            path = "/" + path

        return f"{scheme}://{host}:{port}{path}"

    def connect_ws(self):
        url = self.build_ws_url()
        timeout = float(self.alpha.get("socket_timeout_seconds", 2))
        subprotocols = self.alpha.get("subprotocols") or None

        sslopt = {}
        if self.alpha.get("scheme") == "wss" and self.alpha.get("insecure_tls", False):
            sslopt = {"cert_reqs": ssl.CERT_NONE}

        LOG.info("Connecting WebSocket %s subprotocols=%s", url, subprotocols)

        self.current_ws = websocket.create_connection(
            url,
            timeout=timeout,
            subprotocols=subprotocols,
            sslopt=sslopt,
        )

        LOG.info("WebSocket connected")

    def close_ws(self):
        try:
            if self.current_ws is not None:
                self.current_ws.close()
        except Exception:
            pass
        finally:
            self.current_ws = None

    def ws_send(self, message: str):
        if STOP.is_set():
            return

        LOG.debug("WS send: %s", message)
        self.current_ws.send(message)

    def ws_recv_json(self) -> Optional[Dict[str, Any]]:
        try:
            raw = self.current_ws.recv()
        except websocket.WebSocketTimeoutException:
            return None
        except KeyboardInterrupt:
            STOP.set()
            return None
        except Exception as exc:
            if not STOP.is_set():
                LOG.warning("WebSocket receive failed: %s", exc)
            raise

        if raw is None:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")

        text = str(raw).strip()
        log_chars = int(self.alpha.get("log_raw_chars", 400))
        LOG.debug("WS recv raw: %s", text[:log_chars])

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            LOG.debug("Ignoring non-JSON WebSocket message")
            return None

    def reset_session_state(self):
        self.nav_name_to_id.clear()
        self.latest_by_id.clear()
        self.content_entries.clear()

        for item in self.values:
            item.resolved_id = None

    def walk_navigation(self, data: Any):
        if isinstance(data, dict):
            node_id = data.get("id")
            name = data.get("name")

            if node_id and name:
                self.nav_name_to_id[str(name)] = str(node_id)

            for child in data.get("items", []) or []:
                self.walk_navigation(child)

        elif isinstance(data, list):
            for item in data:
                self.walk_navigation(item)

    def walk_content(self, data: Any, path: Optional[List[str]] = None):
        path = path or []

        if isinstance(data, dict):
            node_id = data.get("id")
            name = str(data.get("name") or "")
            has_value = "value" in data
            children = data.get("items", []) or []

            if has_value and node_id:
                section = path[-1] if path else ""

                self.latest_by_id[str(node_id)] = data.get("value")
                self.content_entries.append(
                    ContentEntry(
                        node_id=str(node_id),
                        name=name,
                        section=section,
                        section_path=list(path),
                    )
                )

            if children:
                next_path = path

                if name and not has_value:
                    next_path = path + [name]

                for child in children:
                    self.walk_content(child, next_path)

        elif isinstance(data, list):
            for item in data:
                self.walk_content(item, path)

    def walk_values(self, data: Any):
        if isinstance(data, dict):
            node_id = data.get("id")

            if node_id and "value" in data:
                self.latest_by_id[str(node_id)] = data.get("value")

            for child in data.get("items", []) or []:
                self.walk_values(child)

        elif isinstance(data, list):
            for item in data:
                self.walk_values(item)

    def handle_message(self, data: Optional[Dict[str, Any]]):
        if not data:
            return

        typ = message_type(data)
        LOG.debug("WS JSON type=%s", typ or "?")

        if typ == "Navigation":
            self.walk_navigation(data)
            LOG.info("Navigation items discovered: %s", len(self.nav_name_to_id))
            return

        if typ == "Content":
            before = len(self.content_entries)
            self.walk_content(data)
            after = len(self.content_entries)
            LOG.info("Content parsed: entries %s -> %s", before, after)
            self.resolve_configured_values()
            return

        if typ == "values":
            before = len(self.latest_by_id)
            self.walk_values(data)
            after = len(self.latest_by_id)
            LOG.info("Values parsed: known ids %s -> %s", before, after)
            return

        self.walk_values(data)

    def get_information_id(self) -> Optional[str]:
        wanted = norm(self.alpha.get("information_menu_name", "Informatie"))

        for name, node_id in self.nav_name_to_id.items():
            if norm(name) == wanted:
                return node_id

        return None

    def send_information_get(self):
        info_id = self.get_information_id()

        if not info_id:
            raise RuntimeError("Could not find Informatie menu id from Navigation")

        self.ws_send(f"GET;{info_id}")

    def resolve_configured_values(self):
        for item in self.values:
            matches: List[ContentEntry] = []

            for entry in self.content_entries:
                if item.name and norm(entry.name) != norm(item.name):
                    continue

                if item.section and norm(entry.section) != norm(item.section):
                    continue

                if item.section_path and not path_suffix_matches(entry.section_path, item.section_path):
                    continue

                matches.append(entry)

            if len(matches) >= item.occurrence:
                selected = matches[item.occurrence - 1]

                if item.resolved_id != selected.node_id:
                    LOG.info(
                        "Resolved configured topic %s: %s / %s occurrence %s -> %s",
                        item.topic,
                        item.section_path or item.section,
                        item.name,
                        item.occurrence,
                        selected.node_id,
                    )

                item.resolved_id = selected.node_id
            else:
                LOG.warning(
                    "Could not resolve configured topic %s: section=%s section_path=%s name=%s occurrence=%s matches=%s",
                    item.topic,
                    item.section,
                    item.section_path,
                    item.name,
                    item.occurrence,
                    len(matches),
                )

    def publish_values(self):
        """
        Publishes MQTT state ONLY for configured YAML values.
        It never publishes one topic per WebSocket id.
        """
        published = 0
        missing = []

        for item in self.values:
            if not item.resolved_id:
                missing.append(f"{item.topic}: unresolved")
                continue

            if item.resolved_id not in self.latest_by_id:
                missing.append(f"{item.topic}: id {item.resolved_id} has no value")
                continue

            parsed = parse_state(self.latest_by_id[item.resolved_id])

            self.mqtt.publish(
                item.topic,
                str(parsed["state"]),
                retain=self.retain,
                qos=0,
            )

            LOG.info("Published configured topic: %s = %s", item.topic, parsed["state"])
            published += 1

        self.mqtt.publish(f"{self.base_topic}/status", "online", retain=True)

        if missing:
            LOG.warning("Missing/unresolved configured topics: %s", "; ".join(missing))

        LOG.info("Published %s/%s configured topics", published, len(self.values))

    def session(self):
        self.reset_session_state()
        self.connect_ws()

        login = self.alpha.get("login_message")
        if login:
            self.ws_send(str(login))

        startup_delay = float(self.alpha.get("startup_delay_seconds", 0.5))
        if startup_delay > 0:
            STOP.wait(startup_delay)

        nav_deadline = time.time() + 5
        while not STOP.is_set() and time.time() < nav_deadline:
            data = self.ws_recv_json()
            self.handle_message(data)

            if self.nav_name_to_id:
                break

        if not self.nav_name_to_id:
            raise RuntimeError("No Navigation received after login")

        self.ws_send("REFRESH")
        STOP.wait(0.2)
        self.ws_send("REFRESH")
        STOP.wait(0.2)

        self.send_information_get()

        content_deadline = time.time() + 5
        while not STOP.is_set() and time.time() < content_deadline:
            data = self.ws_recv_json()
            self.handle_message(data)

            if self.content_entries:
                break

        if not self.content_entries:
            raise RuntimeError("No Content received after GET Informatie")

        self.publish_values()

        refresh_interval = float(self.alpha.get("refresh_interval_seconds", 10))
        next_refresh = time.time() + refresh_interval

        while not STOP.is_set():
            now = time.time()

            if now >= next_refresh:
                self.ws_send("REFRESH")
                next_refresh = now + refresh_interval

            data = self.ws_recv_json()
            if data:
                self.handle_message(data)

                if message_type(data) == "values":
                    self.publish_values()

    def run(self):
        self.connect_mqtt()
        self.publish_discovery()

        reconnect_delay = float(self.alpha.get("reconnect_delay_seconds", 10))

        try:
            while not STOP.is_set():
                try:
                    self.session()
                except KeyboardInterrupt:
                    LOG.warning("CTRL+C received")
                    STOP.set()
                    break
                except Exception as exc:
                    if not STOP.is_set():
                        LOG.warning("Session failed: %s", exc)
                finally:
                    self.close_ws()

                if not STOP.is_set():
                    LOG.warning("Reconnect in %.1f seconds", reconnect_delay)
                    STOP.wait(reconnect_delay)

        finally:
            LOG.warning("Stopping")
            STOP.set()
            self.close_ws()
            self.disconnect_mqtt()


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def handle_signal(signum, frame):
    LOG.warning("Signal %s received", signum)
    STOP.set()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="alpha_ws_mqtt.yaml")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    bridge = AlphaWsMqttBridge(load_config(args.config))

    try:
        bridge.run()
    except KeyboardInterrupt:
        LOG.warning("CTRL+C received in main")
        STOP.set()
        bridge.close_ws()
        bridge.disconnect_mqtt()


if __name__ == "__main__":
    main()