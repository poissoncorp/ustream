from __future__ import annotations
import base64
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from threading import Event
from typing import Dict, List, Tuple, Optional

from aiohttp import web
import socketio
import hashlib
# import cv2
# from mmap import mmap
# import numpy as np

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


def encode_h264(data: bytes) -> bytes:
    encoded_data = data  # todo: h.264 https://github.com/opencv/opencv-python/issues/100#issuecomment-554870068
    return encoded_data

    # # Create a memory-mapped file
    # size = len(bytes) * 10
    # with open("output.avi", "wb") as f:
    #     f.write(b'\x00' * size)
    # with open("output.avi", "r+b") as f:
    #     mmapped_file = mmap(f.fileno(), 0)
    #
    # # Create a VideoWriter object
    # fourcc = cv2.VideoWriter_fourcc(*'H264')
    # fps = 30.0
    # frameSize = (640, 480)
    # out = cv2.VideoWriter(mmapped_file, fourcc, fps, frameSize, isColor=True)
    #
    # # Convert the bytes object to a numpy array
    # frame = cv2.imdecode(np.frombuffer(data, np.uint8), -1)
    #
    # # Write the frame to the VideoWriter object
    # out.write(frame)
    #
    # # Release the VideoWriter object
    # out.release()
    #
    # return mmapped_file.read()


def encrypt_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_data_h264_b64(data_chunk: bytes) -> bytes:
    data_chunk = encode_h264(data_chunk)
    data_chunk = base64.b64encode(data_chunk)
    return data_chunk


class ProxyMetadata:
    def __init__(self, destination_url: str, path: List[str], hops_left: int = 1, data_processed: bool = False):
        self.destination_url = destination_url
        self.hops_left = hops_left
        self.path = path
        self.data_processed = data_processed

    @classmethod
    def from_json(cls, json_dict: Dict) -> ProxyMetadata:
        return cls(json_dict["DestinationUrl"], json_dict["Path"], json_dict["HopsLeft"], json_dict["DataProcessed"])

    def to_json(self) -> Dict:
        return {
            "DestinationUrl": self.destination_url,
            "Path": self.path,
            "HopsLeft": self.hops_left,
            "DataProcessed": self.data_processed,
        }


class DeliveryConfirmation:
    def __init__(self, chunk_id: int):
        self.chunk_id = chunk_id

    @classmethod
    def from_json(cls, json_dict: Dict) -> DeliveryConfirmation:
        return cls(json_dict["ChunkId"])

    def to_json(self):
        return {"ChunkId": self.chunk_id}


class Session:
    def __init__(self, sid: str):
        self.sid = sid
        self.chunk_jsons_bucket: List[Dict] = []
        self.confirmations: List[DeliveryConfirmation] = []


class SingleSocketClient:
    def __init__(self, url: str):
        self.sio = socketio.Client()
        self.url = url
        self.session: Optional[Session] = None

        @self.sio.event
        def connect():
            self._log_info(f"Connected with {self.sio.connection_url}.")

        @self.sio.event
        def disconnect():
            if self.session:
                self.close_session()
            self._log_info(f"Disconnected with {self.url}.")

    def _log_info(self, message: str):
        print(f"[ {self.url} ] " + message)

    def open_session(self):
        self.session = Session(uuid.uuid4().__str__())
        self._log_info(f"Established session with id '{self.session.sid}'.")

    def close_session(self):
        if self.session.chunk_jsons_bucket:
            self._log_info(f"WARNING! {len(self.session.chunk_jsons_bucket)} CHUNKS LEFT AT SESSION CLOSE!")
        self._log_info(f"Session '{self.session.sid}' closed.")
        self.session = None

    def _send_chunk_to_server(self, data_chunk: UstreamChunk) -> UstreamChunk:
        self._log_info(f"Sending data: '{data_chunk.to_json()}'")
        result = []
        e = Event()

        def __set_value(val):
            result.append(val)
            e.set()

        self.sio.emit(
            "process",
            data_chunk.to_json(),
            callback=__set_value,
        )

        e.wait(5)
        return UstreamChunk.from_json(result.pop())

    def _send_chunk_to_server_proxy(self, data_chunk: UstreamChunk, proxy_metadata: ProxyMetadata) -> str:
        self._log_info(f"Sending data (proxy): '{data_chunk.to_json()}'")
        return self.send_chunk_to_server_proxy_pass(data_chunk.to_json(), proxy_metadata)

    def send_chunk_to_server_proxy_pass(self, data_chunk_json: Dict, proxy_metadata: ProxyMetadata) -> str:
        result = []
        delivered_or_failed = Event()

        def __set_value():
            delivered_or_failed.set()

        self.sio.emit("proxy_pass", (data_chunk_json, proxy_metadata.to_json()), callback=__set_value)

        delivered_or_failed.wait(10)
        error_message = ""
        return error_message

    def proxy_take(self, data_chunk_json: Dict) -> str:
        result = []
        delivered_or_failed = Event()

        def __set_value(val):
            result.append(val)
            delivered_or_failed.set()

        self.sio.emit("proxy_take", data_chunk_json, callback=__set_value)
        delivered_or_failed.wait()

        error_message = result.pop() if result else None
        return error_message

    def process_chunks(self, chunks: List[UstreamChunk]) -> List[UstreamChunk]:
        return [self._send_chunk_to_server(chunk) for chunk in chunks]

    def process_chunks_proxy(self, chunks: List[UstreamChunk], proxy_metadata: ProxyMetadata) -> List[UstreamChunk]:
        # jeden po drugim, czekam na error
        errors = [self._send_chunk_to_server_proxy(chunk, proxy_metadata) for chunk in chunks]
        return [UstreamChunk.from_json(chunk_json) for chunk_json in self.session.chunk_jsons_bucket]

class MultiConnectionClient:
    def __init__(self, single_socket_clients: List[SingleSocketClient] = None):
        self._single_socket_clients = single_socket_clients or []
        self._thread_pool_executor = ThreadPoolExecutor(thread_name_prefix="Hydra")

    @classmethod
    def from_urls(cls, nodes_urls: List[str]) -> MultiConnectionClient:
        clients = []
        for url in nodes_urls or ["http://127.0.0.1:2137"]:
            client = SingleSocketClient(url)
            clients.append(client)
        return cls(clients)

    @property
    def single_socket_clients(self) -> List[SingleSocketClient]:
        return self._single_socket_clients

    def _add_node(self, node_url: str):
        client = SingleSocketClient(node_url)
        self._single_socket_clients.append(client)

    def _get_chunks_indexes_ranges_for_multi_send(self, chunks_count: int) -> List[Tuple[int, int]]:
        chunks_per_client = chunks_count // len(self._single_socket_clients)
        if chunks_count % len(self._single_socket_clients) != 0:
            chunks_per_client += 1

        start_end_tuples = []

        # the last one will be shorter, so its end will be set to the end of batch manually to prevent IndexErrors
        for i in range(len(self._single_socket_clients) - 1):
            start_end_tuples.append((chunks_per_client * i, chunks_per_client * (i + 1)))
        start_end_tuples.append((start_end_tuples[-1][1], chunks_count))

        return start_end_tuples

    def split_chunks_into_batches_and_process_them_on_many_nodes_async(
            self, chunks: List[UstreamChunk], proxy_metadata: Optional[ProxyMetadata] = None
    ) -> List[UstreamChunk]:
        # To skip batches allocation time, it'll only read "chunks" part by part
        bounds = (
            [(0, len(chunks))]
            if len(self._single_socket_clients) == 1
            else self._get_chunks_indexes_ranges_for_multi_send(len(chunks))
        )

        # Check if splitting went correctly and all data is going to be processed
        if len(bounds) != len(self._single_socket_clients):
            raise RuntimeError("Batches count doesn't match clients count.")

        results: List[UstreamChunk] = []
        # Connect all clients
        for client in self._single_socket_clients:
            client.sio.connect(client.url)

        # Submit tasks to different threads (every thread sends batch of input data to unique node)
        # Each thread fetches a batch of processed UstreamChunks from the peers (servers)

        def _process_batch(i):  # Where 'i' is the client index & its' destined bounds index
            suitable_client = self.single_socket_clients[i]
            suitable_client.open_session()

            start = bounds[i][0]
            end = bounds[i][1]
            if proxy_metadata is not None:
                res = suitable_client.process_chunks_proxy(chunks[start:end], proxy_metadata)
            else:
                res = suitable_client.process_chunks(chunks[start:end])
            suitable_client.close_session()
            return res

        futures = [self._thread_pool_executor.submit(lambda i: _process_batch(i), i) for i in range(len(bounds))]

        # Wait for all threads
        for future in futures:
            results.extend(future.result())

        self.close_all_connections()
        return results

    def get_processed_bytes(self, data: bytes, proxy_metadata: Optional[ProxyMetadata]) -> bytes:
        # List of unprocessed UstreamChunks - every with 'RAW' status
        chunks = chop_bytes_into_chunks(data)

        # List of encoded UstreamChunks - wait until every chunk will have 'ENCODED' status
        chunks = self.split_chunks_into_batches_and_process_them_on_many_nodes_async(chunks, proxy_metadata)

        return chunks_to_bytes(chunks)

    def close_all_connections(self):
        # Disconnect all clients
        for client in self._single_socket_clients:
            client.sio.disconnect()

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

        @self.server.on("proxy_take")
        def session_proxy_take(sid, data: Dict, proxy_info: Dict) -> str:
            proxy_info = ProxyMetadata.from_json(proxy_info)
            origin_client = self.get_client_to_url(proxy_info.path[0])
            origin_client.session.chunk_jsons_bucket.append(data)

            part_id = data["part_number"]
            confirmation = DeliveryConfirmation(part_id)

            origin_client.session.confirmations.append(confirmation)
            return ""

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
                # if public_url == proxy_metadata.destination_url:
                #     return session_proxy_take(sid, data, proxy_metadata.to_json())
                error_message = client.proxy_take(data)
            else:
                client = self.choose_closest_available_client(proxy_metadata.path)
                error_message = client.send_chunk_to_server_proxy_pass(data, proxy_metadata)

            return error_message


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
