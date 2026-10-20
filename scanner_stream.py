import base64
import gzip
import json
import random
import time
from typing import Iterator


class MockScannerStream:
    """
    Simulates a raw wire stream from distributed Censys scanner nodes.
    Yields raw `bytes` representing wire packets.
    """

    def __init__(self, count: int = 20, seed: int | None = 42):
        self.count = count
        self.generated = 0
        if seed is not None:
            random.seed(seed)
        
        # Track a fixed target to intentionally produce out-of-order versions
        self.special_host_id = "target-host-443"
        self._emitted_v2 = False

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        if self.generated >= self.count:
            raise StopIteration

        self.generated += 1
        return self._generate_wire_packet()

    def _pack_envelope(self, payload_dict: dict, schema_version: int = 1) -> bytes:
        """Helper to create a valid wire packet: dict -> JSON -> gzip -> Base64 -> Envelope."""
        raw_json_bytes = json.dumps(payload_dict).encode("utf-8")
        compressed_bytes = gzip.compress(raw_json_bytes)
        b64_str = base64.b64encode(compressed_bytes).decode("ascii")

        envelope = {
            "schema_version": schema_version,
            "compression": "gzip",
            "data": b64_str,
        }
        return json.dumps(envelope).encode("utf-8")

    def _generate_wire_packet(self) -> bytes:
        # 1. Out-of-order scenario: emit version 2 first, then emit delayed version 1 later
        if not self._emitted_v2 and self.generated == 3:
            self._emitted_v2 = True
            return self._pack_envelope({
                "id": self.special_host_id,
                "ip": "10.0.0.1",
                "port": 443,
                "status": "OPEN",
                "version": 2,
                "banner": "nginx/1.24.0",
                "updated_at": time.time(),
            })

        if self._emitted_v2 and self.generated == 8:
            # Stale packet arriving late: version 1 for the same host
            return self._pack_envelope({
                "id": self.special_host_id,
                "ip": "10.0.0.1",
                "port": 443,
                "status": "CLOSED",
                "version": 1,
                "banner": "unknown",
                "updated_at": time.time() - 300,
            })

        # 2. Corrupted packet scenarios (Poison Pills)
        # Bad Base64 (corrupted transport frame)
        if self.generated == 5:
            corrupt_envelope = {
                "envelope_version": 1,
                "compression": "gzip",
                "data": "!!!corrupted_base64_data@@@",
            }
            return json.dumps(corrupt_envelope).encode("utf-8")

        # Bad Gzip (valid Base64, but corrupted gzip binary)
        if self.generated == 12:
            fake_b64 = base64.b64encode(b"not_a_real_gzip_stream").decode("ascii")
            corrupt_envelope = {
                "envelope_version": 1,
                "compression": "gzip",
                "data": fake_b64,
            }
            return json.dumps(corrupt_envelope).encode("utf-8")

        # 3. Standard valid scan
        host_id = f"host-{random.randint(100, 999)}"
        ports = [80, 443, 22, 8080]
        payload = {
            "id": host_id,
            "ip": f"192.168.1.{random.randint(1, 254)}",
            "port": random.choice(ports),
            "status": "OPEN",
            "version": random.randint(1, 5),
            "banner": f"HTTP/1.1 200 OK Server: Apache/{random.randint(1, 9)}",
            "updated_at": time.time(),
        }
        return self._pack_envelope(payload)