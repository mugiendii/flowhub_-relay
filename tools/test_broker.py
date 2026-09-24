#!/usr/bin/env python3
"""
A minimal MQTT 3.1.1 broker, for tests only.

Exists so the MQTT path can be exercised offline without installing mosquitto
and, more importantly, without pointing test traffic at the production broker.
It binds 127.0.0.1 by default and nothing else.

Supports what this project's traffic actually uses: CONNECT, SUBSCRIBE,
PUBLISH at QoS 0 and 1, PUBACK, PINGREQ, DISCONNECT, retained messages, and
`+`/`#` wildcards. It is NOT a general broker — no sessions, no QoS 2, no will,
no auth. Do not deploy it.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Dict, List, Set, Tuple

log = logging.getLogger("broker")

CONNECT, CONNACK, PUBLISH, PUBACK = 1, 2, 3, 4
SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK = 8, 9, 10, 11
PINGREQ, PINGRESP, DISCONNECT = 12, 13, 14


def encode_remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n:
            digit |= 0x80
        out.append(digit)
        if not n:
            return bytes(out)


async def read_remaining_length(reader: asyncio.StreamReader) -> int:
    multiplier, value = 1, 0
    while True:
        byte = (await reader.readexactly(1))[0]
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value
        multiplier *= 128
        if multiplier > 128 ** 3:
            raise ValueError("malformed remaining length")


def topic_matches(filt: str, topic: str) -> bool:
    """MQTT wildcard matching: `+` is one level, `#` is the rest."""
    f, t = filt.split("/"), topic.split("/")
    for i, part in enumerate(f):
        if part == "#":
            return True
        if i >= len(t):
            return False
        if part != "+" and part != t[i]:
            return False
    return len(f) == len(t)


class Client:
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer
        self.subs: Set[str] = set()
        self.client_id = "?"


class Broker:
    def __init__(self) -> None:
        self.clients: List[Client] = []
        self.retained: Dict[str, Tuple[bytes, int]] = {}

    async def deliver(self, topic: str, payload: bytes) -> None:
        for c in list(self.clients):
            if not any(topic_matches(f, topic) for f in c.subs):
                continue
            try:
                await self.send_publish(c, topic, payload)
            except Exception:
                pass  # a client that went away is cleaned up on its own task

    async def send_publish(self, c: Client, topic: str, payload: bytes) -> None:
        tb = topic.encode()
        body = len(tb).to_bytes(2, "big") + tb + payload   # QoS 0 downstream
        c.writer.write(bytes([PUBLISH << 4]) + encode_remaining_length(len(body)) + body)
        await c.writer.drain()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = Client(writer)
        self.clients.append(client)
        try:
            while True:
                header = await reader.readexactly(1)
                ptype, flags = header[0] >> 4, header[0] & 0x0F
                length = await read_remaining_length(reader)
                body = await reader.readexactly(length) if length else b""

                if ptype == CONNECT:
                    # Skip protocol name/level/flags/keepalive, then read the id.
                    i = 2 + int.from_bytes(body[0:2], "big") + 4
                    idlen = int.from_bytes(body[i:i + 2], "big")
                    client.client_id = body[i + 2:i + 2 + idlen].decode(errors="replace")
                    writer.write(bytes([CONNACK << 4, 2, 0, 0]))
                    await writer.drain()
                    log.info("connect: %s", client.client_id)

                elif ptype == SUBSCRIBE:
                    packet_id = body[0:2]
                    i, granted = 2, []
                    while i < len(body):
                        tl = int.from_bytes(body[i:i + 2], "big")
                        topic = body[i + 2:i + 2 + tl].decode()
                        qos = body[i + 2 + tl]
                        client.subs.add(topic)
                        granted.append(min(qos, 1))
                        i += 3 + tl
                    payload = packet_id + bytes(granted)
                    writer.write(bytes([SUBACK << 4]) + encode_remaining_length(len(payload)) + payload)
                    await writer.drain()
                    log.info("subscribe: %s -> %s", client.client_id, sorted(client.subs))
                    # Retained messages are delivered on subscribe. The device's
                    # capability report relies on this, so a relay restart does
                    # not have to wait for the next publish.
                    for topic, (payload_b, _q) in self.retained.items():
                        if any(topic_matches(f, topic) for f in client.subs):
                            await self.send_publish(client, topic, payload_b)

                elif ptype == PUBLISH:
                    qos = (flags >> 1) & 0x03
                    retain = flags & 0x01
                    tl = int.from_bytes(body[0:2], "big")
                    topic = body[2:2 + tl].decode()
                    i = 2 + tl
                    packet_id = b""
                    if qos > 0:
                        packet_id = body[i:i + 2]
                        i += 2
                    payload = body[i:]

                    if retain:
                        # An empty retained payload clears it, per the spec.
                        if payload:
                            self.retained[topic] = (payload, qos)
                        else:
                            self.retained.pop(topic, None)

                    if qos == 1 and packet_id:
                        writer.write(bytes([PUBACK << 4, 2]) + packet_id)
                        await writer.drain()

                    log.info("publish: %s (%d bytes)%s", topic, len(payload),
                             " [retained]" if retain else "")
                    await self.deliver(topic, payload)

                elif ptype == UNSUBSCRIBE:
                    packet_id = body[0:2]
                    i = 2
                    while i < len(body):
                        tl = int.from_bytes(body[i:i + 2], "big")
                        client.subs.discard(body[i + 2:i + 2 + tl].decode())
                        i += 2 + tl
                    writer.write(bytes([UNSUBACK << 4, 2]) + packet_id)
                    await writer.drain()

                elif ptype == PINGREQ:
                    writer.write(bytes([PINGRESP << 4, 0]))
                    await writer.drain()

                elif ptype == DISCONNECT:
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            if client in self.clients:
                self.clients.remove(client)
            writer.close()
            log.info("disconnect: %s", client.client_id)


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18830)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO,
                        format="%(asctime)s broker  %(message)s")
    broker = Broker()
    server = await asyncio.start_server(broker.handle, args.host, args.port)
    log.info("listening on %s:%d (tests only - do not deploy)", args.host, args.port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
