from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import List, Tuple, Dict, Optional

import socketio

from ustream.frames import FrameBlob, break_stream_into_frames, frames_to_stream
from ustream.info import ProxyMetadata, DeliveryConfirmation


class Session:
    def __init__(self, sid: str):
        self.sid = sid
        self.frames_blobs_jsons_bucket: List[Dict] = []
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
        if self.session.frames_blobs_jsons_bucket:
            self._log_info(f"WARNING! {len(self.session.frames_blobs_jsons_bucket)} FRAMES LEFT AT SESSION CLOSE!")
        self._log_info(f"Session '{self.session.sid}' closed.")
        self.session = None

    def _send_frame_to_server(self, frame_blob: FrameBlob) -> FrameBlob:
        if len(frame_blob.data) < 20000:
            self._log_info(f"Sending data: '{frame_blob.to_json()}'")
        result = []
        e = Event()

        def __set_value(val):
            result.append(val)
            e.set()

        self.sio.emit(
            "process",
            frame_blob.to_json(),
            callback=__set_value,
        )

        e.wait(5)
        return FrameBlob.from_json(result.pop())

    def _send_frame_to_server_proxy(
        self, frame_blob: FrameBlob, proxy_metadata: ProxyMetadata, verbose: bool = False
    ) -> str:
        if verbose:
            self._log_info(f"Sending data (proxy): '{frame_blob.to_json()}'")
        return self.send_frame_to_server_proxy_pass(frame_blob.to_json(), proxy_metadata)

    def send_frame_to_server_proxy_pass(self, frame_blob_json: Dict, proxy_metadata: ProxyMetadata) -> str:
        delivered_or_failed = Event()

        def __set_value():
            delivered_or_failed.set()

        self.sio.emit("proxy_pass", (frame_blob_json, proxy_metadata.to_json()), callback=__set_value)

        delivered_or_failed.wait(10)
        error_message = ""
        return error_message

    def proxy_take(self, frame_blob_json: Dict) -> str:
        result = []
        delivered_or_failed = Event()

        def __set_value(val):
            result.append(val)
            delivered_or_failed.set()

        self.sio.emit("proxy_take", frame_blob_json, callback=__set_value)
        delivered_or_failed.wait()

        error_message = result.pop() if result else None
        return error_message

    def process_frames(self, frames: List[FrameBlob]) -> List[FrameBlob]:
        return [self._send_frame_to_server(frame) for frame in frames]

    def process_frames_proxy(self, frames: List[FrameBlob], proxy_metadata: ProxyMetadata) -> List[FrameBlob]:
        # jeden po drugim, czekam na error
        # todo: errors
        errors = [self._send_frame_to_server_proxy(frame, proxy_metadata) for frame in frames]
        return [FrameBlob.from_json(frame_blob_json) for frame_blob_json in self.session.frames_blobs_jsons_bucket]


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

    def _get_frames_indexes_ranges_for_multi_send(self, frames_count: int) -> List[Tuple[int, int]]:
        frames_per_client = frames_count // len(self._single_socket_clients)
        if frames_count % len(self._single_socket_clients) != 0:
            frames_per_client += 1

        start_end_tuples = []

        # the last one will be shorter, so its end will be set to the end of batch manually to prevent IndexErrors
        for i in range(len(self._single_socket_clients) - 1):
            start_end_tuples.append((frames_per_client * i, frames_per_client * (i + 1)))
        start_end_tuples.append((start_end_tuples[-1][1], frames_count))

        return start_end_tuples

    def split_frames_into_batches_and_process_them_on_many_nodes_async(
        self, frames: List[FrameBlob], proxy_metadata: Optional[ProxyMetadata] = None
    ) -> List[FrameBlob]:
        # calculate the ranges of the frames list
        # they'll indicate which frames will be processed on which nodes
        # range[0] will be like 0-50
        # this indicates that host 0 will process frames 0-50
        bounds = (
            [(0, len(frames))]
            if len(self._single_socket_clients) == 1
            else self._get_frames_indexes_ranges_for_multi_send(len(frames))
        )

        # Check if splitting went correctly and all data is going to be processed
        if len(bounds) != len(self._single_socket_clients):
            raise RuntimeError("Batches count doesn't match clients count.")

        processed_frames: List[FrameBlob] = []
        # Connect all clients
        for client in self._single_socket_clients:
            client.sio.connect(client.url)

        # Submit tasks to different threads (every thread sends batch of input data to unique node)
        # Each thread fetches a batch of processed FrameBlobs from the peers (servers)

        def _process_batch(i):  # Where 'i' is the client index & its' destined bounds index
            suitable_client = self.single_socket_clients[i]
            suitable_client.open_session()

            start = bounds[i][0]
            end = bounds[i][1]
            if proxy_metadata is not None:
                res = suitable_client.process_frames_proxy(frames[start:end], proxy_metadata)
            else:
                res = suitable_client.process_frames(frames[start:end])
            suitable_client.close_session()
            return res

        processed_frames_lists_futures = [
            self._thread_pool_executor.submit(lambda i: _process_batch(i), i) for i in range(len(bounds))
        ]

        # Wait for all threads
        for future in processed_frames_lists_futures:
            processed_frames.extend(future.result())

        self.close_all_connections()
        return processed_frames

    def get_processed_bytes(self, data: bytes, proxy_metadata: Optional[ProxyMetadata]) -> bytes:
        # List of unprocessed FrameBlobs - every with 'RAW' status
        frames = break_stream_into_frames(data)

        # List of encoded UstreamChu/nks - wait until every frame will have 'ENCODED' status
        frames = self.split_frames_into_batches_and_process_them_on_many_nodes_async(frames, proxy_metadata)

        return frames_to_stream(frames)

    def close_all_connections(self):
        # Disconnect all clients
        for client in self._single_socket_clients:
            client.sio.disconnect()
