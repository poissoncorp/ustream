from __future__ import annotations
import base64
import os
from typing import Dict, List

from aiohttp import web
import socketio
import hashlib

from chunks import UstreamChunk, UstreamChunkStatus
from ustream.client import MultiConnectionClient, SingleSocketClient
from ustream.options import ForwardOptions


def encode_h264(data: bytes) -> bytes:
    encoded_data = data  # todo: h.264 https://github.com/opencv/opencv-python/issues/100#issuecomment-554870068
    return encoded_data


def encrypt_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_data_h264_b64(data_chunk: bytes) -> bytes:
    data_chunk = encode_h264(data_chunk)
    data_chunk = base64.b64encode(data_chunk)
    return data_chunk


class ServerNode:
    def __init__(self, host_urls: List[str], public_url: str):
        self.public_url = public_url
        self.server = socketio.AsyncServer()
        self.client: MultiConnectionClient = MultiConnectionClient.from_urls(host_urls)

        @self.server.on("process")
        def process(sid, data: Dict):
            ustream_chunk = UstreamChunk.from_json(data)

            print("Processing data...")
            # code your data processing here...
            processed_data = encode_data_h264_b64(ustream_chunk.data)
            ustream_chunk.data = processed_data
            ustream_chunk.status = UstreamChunkStatus.ENCODED

            print(f"Returning {len(processed_data)} bytes of h.264 & b64 encoded data.")
            return ustream_chunk.to_json()

        @self.server.on("proxy_forward")
        def session_proxy_forward(sid, data: Dict, forward_options: Dict):
            # Unwrap options
            forward_options = ForwardOptions.from_json(forward_options)

            # Sign in to the path
            forward_options.path.append(self.public_url)

            # Process the data if hasn't been processed yet
            if forward_options.data_processed:
                data = process(data)

            # Pick the client to pass the data
            client = self.choose_closest_available_client(forward_options.path)

            # Emit processed data
            client.sio.emit("proxy_take", data, forward_options.to_json())

        @self.server.on("proxy_take")
        def session_proxy_take(sid, data: Dict, forward_options: Dict):
            pass  # todo: Toss the data on the session data pile and send the confirmation of delivery back

    def attach_server_to_app(self, app: web.Application):
        self.server.attach(app)

    def choose_closest_available_client(self, unavailable_nodes_urls: List[str] = None) -> SingleSocketClient:
        unavailable_nodes_urls = unavailable_nodes_urls or []
        for client in self.client.single_socket_clients:
            if client.url in unavailable_nodes_urls:
                continue
            return client


def run_server(urls: List = None):
    # todo: Figure out how to fetch public url from machine. Maybe pre-set env val?
    ustream_public_url = os.environ.get("USTREAM_PUBLIC_URL") or "http://127.0.0.1:2137"

    server_node = ServerNode(urls or [], ustream_public_url)
    app = web.Application()
    server_node.attach_server_to_app(app)

    # Take the port from the environmental variable
    app_port = int(ustream_public_url.split(":")[-1])
    web.run_app(app, port=app_port)


if __name__ == "__main__":
    run_server()
