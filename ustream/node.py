from __future__ import annotations
import os
import cv2
import base64
import hashlib
import socketio
import numpy as np
from aiohttp import web
from typing import Dict, List, Callable
from frames import FrameBlob, FrameStatus
from ustream.info import ProxyMetadata, DeliveryConfirmation
from ustream.client import MultiConnectionClient, SingleSocketClient


def encode_h264(frame_blob: FrameBlob) -> FrameBlob:
    fourcc = cv2.VideoWriter_fourcc(*"h264")
    output_file = os.path.join(os.getcwd(), "output.mp4")
    out = cv2.VideoWriter(
        output_file, fourcc, frame_blob.frame_rate, (frame_blob.resolution_x, frame_blob.resolution_y)
    )

    frame = np.frombuffer(frame_blob.data, dtype=np.uint8).reshape(
        (frame_blob.resolution_y, frame_blob.resolution_x, frame_blob.color_depth_in_bytes)
    )
    out.write(frame)

    out.release()
    with open(output_file, "rb") as f:
        frame_blob.data = f.read()
    os.remove(output_file)

    return frame_blob


def encrypt_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_data_h264_b64(frame_blob: FrameBlob) -> FrameBlob:
    frame_blob = encode_h264(frame_blob)
    frame_blob.status = FrameStatus.ENCODED
    frame_blob.data = base64.b64encode(frame_blob.data)
    frame_blob.data_length = len(frame_blob.data)
    return frame_blob


class ServerNode:
    def __init__(
        self,
        host_urls: List[str],
        public_url: str,
        process_callback: Callable[[FrameBlob], FrameBlob] = encode_data_h264_b64,
    ):
        self.public_url = public_url
        self.server = socketio.AsyncServer(max_http_buffer_size=1920 * 1080 * 32)
        self.client = MultiConnectionClient.from_urls(host_urls)
        self.process_callback = process_callback

        @self.server.on("process")
        def process(sid, data: Dict) -> Dict:
            frame_blob = FrameBlob.from_json(data)

            print("Processing data...")
            # code your data processing here...
            frame_blob = self.process_callback(frame_blob)

            print(f"Returning {len(frame_blob.data)} bytes of h.264 & b64 encoded data.")
            return frame_blob.to_json()

        @self.server.on("proxy_pass")
        def session_proxy_pass(sid, data: Dict, proxy_metadata_json: Dict) -> DeliveryConfirmation:
            # Unwrap options and frame blob
            proxy_metadata = ProxyMetadata.from_json(proxy_metadata_json)
            frame_blob = FrameBlob.from_json(data)

            # Sign in to the path
            proxy_metadata.path.append(self.public_url)
            proxy_metadata.hops_left -= 1

            # Process the data if hasn't been processed yet
            if frame_blob.status == FrameStatus.RAW:
                data = process(None, data)

            # Pick the client to pass the data & emit the data
            if proxy_metadata.hops_left == 0:
                client = self.get_client_to_url(proxy_metadata.destination_url)
                delivery_confirmation = client.proxy_touchdown(data, proxy_metadata)
            else:
                client = self.choose_closest_available_client(proxy_metadata.path)
                delivery_confirmation = client.proxy_pass(data, proxy_metadata)

            return delivery_confirmation

        @self.server.on("proxy_touchdown")
        def session_proxy_touchdown(sid, data: Dict, proxy_metadata: Dict):
            proxy_metadata = ProxyMetadata.from_json(proxy_metadata)
            origin_client = self.get_client_to_url(proxy_metadata.path[0])
            origin_client.session.frames_blobs_jsons_bucket.append(data)

            part_id = data["part_number"]
            confirmation = DeliveryConfirmation(part_id)
            return confirmation

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
