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
        color_depth_in_bytes: int = constants.default_color_depth_bytes,
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


def stream_to_frames(data: bytes, accept_incomplete_frame_data: bool = False) -> List[FrameBlob]:
    if not accept_incomplete_frame_data and len(data) % FRAME_SIZE != 0:
        raise ValueError("Data isn't complete. Total length of the stream bytes data isn't divisible by frame length.")

    frames = []
    for i in range(int(len(data) / FRAME_SIZE)):
        cursor = i * FRAME_SIZE
        next_frame_data = data[cursor : cursor + FRAME_SIZE]
        frames.append(FrameBlob(next_frame_data, i))

    return frames


def frames_to_stream(frames: List[FrameBlob]) -> bytes:
    frames.sort(key=lambda x: x.frame_id)
    frames_bytes = [frame.data for frame in frames]
    frames_bytes_b64_decoded = [base64.b64decode(encoded_frame_bytes) for encoded_frame_bytes in frames_bytes]
    return b"".join(frames_bytes_b64_decoded)
