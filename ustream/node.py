from __future__ import annotations
import base64
import os
from typing import Dict, List, Tuple

from aiohttp import web
import socketio
import hashlib

from chunks import UstreamChunk, UstreamChunkStatus
from ustream.client import MultiConnectionClient, SingleSocketClient
from ustream.info import ProxyMetadata, DeliveryConfirmation


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
        def process(sid, data: Dict) -> Dict:
            ustream_chunk = UstreamChunk.from_json(data)

            print("Processing data...")
            # code your data processing here...
            processed_data = encode_data_h264_b64(ustream_chunk.data)
            ustream_chunk.data = processed_data
            ustream_chunk.status = UstreamChunkStatus.ENCODED

            print(f"Returning {len(processed_data)} bytes of h.264 & b64 encoded data.")
            return ustream_chunk.to_json()

        @self.server.on("proxy_pass")
        def session_proxy_pass(sid, data: Dict, proxy_metadata_json: Dict) -> str:
            # Unwrap options
            proxy_metadata = ProxyMetadata.from_json(proxy_metadata_json)

            # Sign in to the path
            proxy_metadata.path.append(self.public_url)
            proxy_metadata.hops_left -= 1

            # Process the data if hasn't been processed yet
            if not proxy_metadata.data_processed:
                data = process(None, data)

            # Pick the client to pass the data & emit the data
            if proxy_metadata.hops_left == 0:
                client = self.get_client_to_url(proxy_metadata.destination_url)
                error_message = client.proxy_take(data)
            else:
                client = self.choose_closest_available_client(proxy_metadata.path)
                error_message = client.send_chunk_to_server_proxy_pass(data, proxy_metadata)

            return error_message

        @self.server.on("proxy_take")
        def session_proxy_take(sid, data: Dict, proxy_info: Dict):
            proxy_info = ProxyMetadata.from_json(proxy_info)
            origin_client = self.get_client_to_url(proxy_info.path[0])
            origin_client.session.chunk_jsons_bucket.append(data)

            part_id = data["part_number"]
            confirmation = DeliveryConfirmation(part_id)

            origin_client.session.confirmations.append(confirmation)
            return proxy_info

    def attach_server_to_app(self, app: web.Application):
        self.server.attach(app)

    def choose_closest_available_client(self, unavailable_nodes_urls: List[str] = None) -> SingleSocketClient:
        unavailable_nodes_urls = unavailable_nodes_urls or []
        for client in self.client.single_socket_clients:
            if client.url in unavailable_nodes_urls:
                continue
            return client

    def get_client_to_url(self, destination_url: str) -> SingleSocketClient:
        for client in self.client.single_socket_clients:
            if client.url != destination_url:
                continue
            return client


def run_server(urls: List = None):
    ustream_public_url = os.environ.get("USTREAM_PUBLIC_URL") or "http://127.0.0.1:2137"

    server_node = ServerNode(urls or [], ustream_public_url)
    app = web.Application()
    server_node.attach_server_to_app(app)

    # Take the port from the environmental variable
    app_port = int(ustream_public_url.split(":")[-1])
    web.run_app(app, port=app_port)


if __name__ == "__main__":
    run_server()
