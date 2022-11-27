import base64
from typing import Dict

from aiohttp import web
import socketio
import hashlib

from chunks import UstreamChunk, UstreamChunkStatus

sio = socketio.AsyncServer()
app = web.Application()
sio.attach(app)


class Session:
    def __init__(self, sid: str):
        self.sid = sid
        self.chunks_parsed: int = 0


def encode_h264(data: bytes) -> bytes:
    encoded_data = data  # todo: h.264 https://github.com/opencv/opencv-python/issues/100#issuecomment-554870068
    return encoded_data


def encrypt_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_data_h264_b64(data_chunk: bytes) -> bytes:
    data_chunk = encode_h264(data_chunk)
    data_chunk = base64.b64encode(data_chunk)
    return data_chunk


@sio.on("session_create")
async def create_session(sid):
    return f"Created new session of id '{sid}'"


@sio.on("session_process")
def session_process(sid, data: Dict):
    ustream_chunk = UstreamChunk.from_json(data)

    print("Processing data...")
    processed_data = encode_data_h264_b64(ustream_chunk.data)
    ustream_chunk.data = processed_data
    ustream_chunk.status = UstreamChunkStatus.ENCODED

    print(f"Returning {len(processed_data)} bytes of h.264 & b64 encoded data.")
    return ustream_chunk.to_json()


@sio.on("session_close")
def disconnect(sid):
    return f"Session of id '{sid}' has been closed"


def run_server():
    web.run_app(app, port=2137)


if __name__ == "__main__":
    run_server()
