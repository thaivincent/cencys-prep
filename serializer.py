import base64
from enum import Enum
import gzip
from dataclasses import dataclass
import json
from typing import Any

@dataclass(frozen=True, slots=True)
class ScanRecord:
    id: str
    ip: str
    port: int
    status: str
    version: int
    updated_at: float
    banner: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "ip": self.ip,
            "port": self.port,
            "status": self.status,
            "version": self.version,
            "updated_at": self.updated_at,
            "banner": self.banner,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScanRecord":
        return cls(
            id=data["id"],
            ip=data["ip"],
            port=data["port"],
            status=data["status"],
            version=data["version"],
            updated_at=data["updated_at"],
            banner=data.get("banner", ""),
        )

@dataclass(frozen=True, slots=True)
class Envelope:
    schema_version: int
    compression: str
    data: str  # Base64-encoded ASCII payload

    def to_bytes(self) -> bytes:
        """Serializes the envelope to UTF-8 JSON bytes for the wire."""
        return json.dumps({
            "schema_version": self.schema_version,
            "compression": self.compression,
            "data": self.data,
        }).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw_bytes: bytes) -> "Envelope":
        """Parses and validates incoming wire bytes into an Envelope."""
        data = json.loads(raw_bytes.decode("utf-8"))
        if "schema_version" not in data or "data" not in data:
            raise ValueError("Malformed envelope: missing required fields")
        return cls(
            schema_version=int(data["schema_version"]),
            compression=data.get("compression", "none"),
            data=data["data"],
        )

class CompressionType(Enum):
    NONE = 1
    GZIP = 2

# Takes a scan record, compresses it, packs it into a envelope, and turns it into bytes
def packMessage(schema : int, compression : CompressionType, record : ScanRecord ) -> bytes:
    compressed = json.dumps(record.to_dict()).encode("utf-8")
    match compression:
        case CompressionType.GZIP:
            compressed = gzip.compress(compressed)
        case CompressionType.NONE:
            pass
        case _:
            raise ValueError(f"Unsuported Compression Type: {compression}" )
    b64_str = base64.b64encode(compressed).decode("ascii")
    
    return Envelope(schema_version=schema, compression=compression.name.lower(), data=b64_str).to_bytes()

# Takes bytes, turns it into an envelope, then turns it into its appropriate scan record schema
def unpackMessage(data:bytes) -> ScanRecord:
    envelope = Envelope.from_bytes(data)
    compressedBytes = base64.b64decode(envelope.data)
    raw_bytes = compressedBytes

    match envelope.compression:
        case "gzip":
            raw_bytes = gzip.decompress(compressedBytes)
        case  "none":
            pass
        case _:
            raise ValueError(f"Unsupported compression: {envelope.compression}")

    payload_dict = json.loads(raw_bytes.decode("utf-8"))
    return ScanRecord.from_dict(payload_dict)
            
