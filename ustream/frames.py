from __future__ import annotations

import base64
from enum import Enum
from typing import List, Dict

from ustream import constants
from ustream.constants import default_color_depth_bytes, default_res_x, default_res_y

FRAME_SIZE = default_res_x * default_res_y * default_color_depth_bytes


class FrameStatus(Enum):
    RAW = "Raw"
    ENCODED = "Encoded"


class FrameBlob:
    def __init__(
        self,
        data: bytes,
        frame_id: int,
        status: FrameStatus = FrameStatus.RAW,
        data_length: int = FRAME_SIZE,
        resolution_x: int = constants.default_res_x,
        resolution_y: int = constants.default_res_y,
        frame_rate: int = constants.default_frame_rate,
        color_depth_in_bytes: int = constants.default_color_depth_bytes
    ):
        self.data = data
        self.status = status
        self.frame_id = frame_id
        self.data_length = data_length
        self.resolution_x = resolution_x
        self.resolution_y = resolution_y
        self.frame_rate = frame_rate
        self.color_depth_in_bytes = color_depth_in_bytes

    def to_json(self) -> Dict:
        return {
            "data": self.data,
            "frame_id": self.frame_id,
            "status": self.status.value,
            "data_length": self.data_length,
            "resolution_x": self.resolution_x,
            "resolution_y": self.resolution_y,
            "frame_rate": self.frame_rate,
            "color_depth_in_bytes": self.color_depth_in_bytes,
        }

    @classmethod
    def from_json(cls, json_dict) -> FrameBlob:
        return cls(
            json_dict["data"],
            json_dict["frame_id"],
            FrameStatus(json_dict["status"]),
            json_dict["data_length"],
            json_dict["resolution_x"],
            json_dict["resolution_y"],
            json_dict["frame_rate"],
            json_dict["color_depth_in_bytes"],
        )


def fill_up_data_with_zeros_to_standard_size(incomplete_frame_data: bytes) -> bytes:
    incomplete_frame_data += b"0" * (FRAME_SIZE - len(incomplete_frame_data))
    return incomplete_frame_data


def remove_extra_bytes_if_needed(data_chunk: bytes, true_length: int) -> bytes:
    if len(data_chunk) == true_length:
        return data_chunk
    return data_chunk[:true_length]


def break_stream_into_frames(data: bytes, accept_incomplete_frame_data: bool = False) -> List[FrameBlob]:
    if not accept_incomplete_frame_data and len(data) % FRAME_SIZE != 0:
        raise ValueError("Data isn't complete. Total length of the stream bytes data isn't divisible by frame length.")

    frames = []
    for i in range(int(len(data) / FRAME_SIZE)):
        cursor = i * FRAME_SIZE
        next_frame_data = data[cursor : cursor + FRAME_SIZE]
        frames.append(FrameBlob(next_frame_data, i))

    last_frame_start = len(frames) * FRAME_SIZE
    if last_frame_start == len(data):
        return frames

    # if last frame length isn't equal to the standards - make it equal to the standards or throw error
    last_frame_data = data[last_frame_start : last_frame_start + FRAME_SIZE]
    if accept_incomplete_frame_data:
        last_frame_data = fill_up_data_with_zeros_to_standard_size(last_frame_data)

    frames.append(FrameBlob(last_frame_data, len(frames), data_length=len(last_frame_data)))
    return frames


def frames_to_stream(chunks: List[FrameBlob]) -> bytes:
    chunks.sort(key=lambda x: x.frame_id)
    byte_chunks = [chunk.data for chunk in chunks[:-1]]
    chunks_decoded = [base64.b64decode(encoded_chunk) for encoded_chunk in byte_chunks]
    body = b"".join(chunks_decoded)

    tail_encoded = chunks[-1].data
    tail_decoded = base64.b64decode(tail_encoded)
    tail = remove_extra_bytes_if_needed(tail_decoded, chunks[-1].data_length)
    return body + tail
