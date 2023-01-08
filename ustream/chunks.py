from __future__ import annotations

import base64
from enum import Enum
from typing import List, Dict, Tuple

CHUNK_SIZE = 190


class UstreamChunkStatus(Enum):
    RAW = "Raw"
    ENCODED = "Encoded"


class UstreamChunk:
    def __init__(
        self,
        chunk: bytes,
        part_number: int,
        status: UstreamChunkStatus = UstreamChunkStatus.RAW,
        length: int = CHUNK_SIZE,
    ):
        self.data = chunk
        self.part_number = part_number
        self.status = status
        self.length = length

    def to_json(self) -> Dict:
        return {
            "data_chunk": self.data,
            "part_number": self.part_number,
            "status": self.status.value,
            "length": self.length,
        }

    @classmethod
    def from_json(cls, json_dict) -> UstreamChunk:
        return cls(
            json_dict["data_chunk"],
            json_dict["part_number"],
            UstreamChunkStatus(json_dict["status"]),
            json_dict["length"],
        )


def add_extra_bytes(short_bytes: bytes) -> Tuple[bytes, int]:
    length = len(short_bytes)
    short_bytes += b"0" * (CHUNK_SIZE - len(short_bytes))
    return short_bytes, length


def remove_extra_bytes_if_needed(data_chunk: bytes, true_length: int) -> bytes:
    if len(data_chunk) == true_length:
        return data_chunk
    return data_chunk[:true_length]


def chop_bytes_into_chunks(data: bytes) -> List[UstreamChunk]:
    chunks = []
    # chopping the 'data' on CHUNK_SIZE'd parts
    for i in range(int(len(data) / CHUNK_SIZE)):
        cursor = i * CHUNK_SIZE
        next_chunk = data[cursor : cursor + CHUNK_SIZE]
        chunks.append(UstreamChunk(next_chunk, i))

    # final chop
    last_chunk_start = len(chunks) * CHUNK_SIZE
    if last_chunk_start == len(data):
        return chunks

    # if last chunk length isn't equal to the standards - make it equal to the standards
    last_chunk = data[last_chunk_start : last_chunk_start + CHUNK_SIZE]
    last_chunk, length = add_extra_bytes(last_chunk)

    chunks.append(UstreamChunk(last_chunk, len(chunks), length=length))

    return chunks


def chunks_to_bytes(chunks: List[UstreamChunk]) -> bytes:
    chunks.sort(key=lambda x: x.part_number)
    byte_chunks = [chunk.data for chunk in chunks[:-1]]
    chunks_decoded = [base64.b64decode(encoded_chunk) for encoded_chunk in byte_chunks]
    body = b"".join(chunks_decoded)

    tail_encoded = chunks[-1].data
    tail_decoded = base64.b64decode(tail_encoded)
    tail = remove_extra_bytes_if_needed(tail_decoded, chunks[-1].length)
    return body + tail
