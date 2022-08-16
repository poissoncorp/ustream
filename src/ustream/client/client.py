import base64
from typing import Dict, List, Callable

import socketio

from src.ustream.chunks import UstreamChunk, bytes_to_chunks, chunks_to_bytes

sio = socketio.Client()


class Session:
    def __init__(self, sid: str):
        self.sid = sid


@sio.event
def connect():
    print("Connected")


@sio.on("session_create")
def my_message(data):
    print(f"Message received with {data}")


@sio.on("*")
def catch_all(event, data):
    print(event)
    print(data)


@sio.on("session_close")
def disconnect():
    print("Disconnected")


def create_session():
    sio.emit("session_create", callback=print)


def close_session():
    sio.emit("session_close", callback=print)



def send_chunk(data_chunk: UstreamChunk) -> UstreamChunk:
    print(f"Sending data: '{data_chunk.to_json()}'")
    result = None

    def __set_value(val):
        nonlocal result
        result = val

    sio.emit(
        "session_process",
        data_chunk.to_json(),
        callback=__set_value,
    )

    return UstreamChunk.from_json(result)


def encode_chunks(chunks: List[UstreamChunk]) -> List[UstreamChunk]:
    parsed_chunks = [send_chunk(chunk) for chunk in chunks]

    return parsed_chunks


def process_data(data: bytes) -> bytes:
    chunks = bytes_to_chunks(data)  # List of unprocessed UstreamChunks - every with 'RAW' status
    chunks = encode_chunks(chunks)  # List of encoded UstreamChunks - wait until every chunk will have 'ENCODED' status
    return chunks_to_bytes(chunks)


if __name__ == "__main__":
    sio.connect("http://127.0.0.1:2137")
    create_session()
    print(process_data(b"twoja babcia"))

