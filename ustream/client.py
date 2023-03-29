from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from threading import Event
from typing import List, Tuple, Dict, Optional

import socketio

from ustream.frames import FrameBlob, stream_to_frames, frames_to_stream
from ustream.info import ProxyMetadata, DeliveryConfirmation


class MicroSession:
    def __init__(self, sid: str):
        self.sid = sid
        self.frames_blobs_bucket: List[FrameBlob] = []


class ConnectionManager:
    def __init__(self, url: str):
        self.sio = socketio.Client()
        self.url = url
        self.current_micro_session: Optional[MicroSession] = None

        @self.sio.event
        def connect():
            self._log_info(f"Established connection with {self.sio.connection_url}. Session id is '{self.sio.sid}'.")

        @self.sio.event
        def disconnect():
            if self.current_micro_session:
                self.close_micro_session()
            self._log_info(f"Disconnected with {self.url}.")

    def _log_info(self, message: str):
        print(f"[ {self.url} ] " + message)

    def open_micro_session(self):
        self.current_micro_session = MicroSession(uuid.uuid4().__str__())
        self._log_info(f"Established micro-session with id '{self.current_micro_session.sid}'.")

    def close_micro_session(self):
        if self.current_micro_session.frames_blobs_bucket:
            self._log_info(
                f"WARNING! {len(self.current_micro_session.frames_blobs_bucket)} FRAMES LEFT AT SESSION CLOSE!"
            )
        self._log_info(f"Micro-session '{self.current_micro_session.sid}' closed.")
        self.current_micro_session = None

    def _process_frame_remotely(self, frame_blob: FrameBlob) -> FrameBlob:
        result = []
        frame_processed = Event()

        def __set_value(val):
            result.append(val)
            frame_processed.set()

        self.sio.emit(
            "process",
            frame_blob.to_json(),
            callback=__set_value,
        )

        frame_processed.wait(5)
        return FrameBlob.from_json(result.pop())

    def _process_frame_remotely_proxy(
        self, frame_blob: FrameBlob, proxy_metadata: ProxyMetadata, verbose: bool = False
    ) -> DeliveryConfirmation:
        if verbose:
            self._log_info(f"Sending data (proxy): '{frame_blob.to_json()}'")
        return self.proxy_pass(frame_blob.to_json(), proxy_metadata)

    def _handle_proxy_pass_error(
        self, ex: Exception, frame_blob_json: Dict, proxy_metadata: ProxyMetadata
    ) -> Optional[DeliveryConfirmation]:
        # A place for the proper error handling implementation
        return None

    def _handle_proxy_touchdown_error(
        self, ex: Exception, frame_blob_json: Dict, proxy_metadata: ProxyMetadata
    ) -> Optional[DeliveryConfirmation]:
        # A place for the proper error handling implementation
        return None

    def proxy_pass(self, frame_blob_json: Dict, proxy_metadata: ProxyMetadata) -> Optional[DeliveryConfirmation]:
        result = []
        delivered = Event()

        def __set_value(val):
            result.append(val)
            delivered.set()

        self.sio.emit("proxy_pass", (frame_blob_json, proxy_metadata.to_json()), callback=__set_value)

        try:
            delivered.wait(10)
            confirmation = result[0]
        except Exception as ex:
            confirmation = self._handle_proxy_pass_error(ex, frame_blob_json, proxy_metadata)
        return confirmation

    def proxy_touchdown(self, frame_blob_json: Dict, proxy_metadata: ProxyMetadata) -> Optional[DeliveryConfirmation]:
        result = []
        delivered = Event()

        def __set_value(val):
            result.append(val)
            delivered.set()

        self.sio.emit("proxy_touchdown", frame_blob_json, callback=__set_value)
        try:
            delivered.wait(10)
            confirmation = result[0]
        except Exception as ex:
            confirmation = self._handle_proxy_touchdown_error(ex, frame_blob_json, proxy_metadata)

        return confirmation

    def process_frames(self, frames: List[FrameBlob]) -> List[FrameBlob]:
        return [self._process_frame_remotely(frame) for frame in frames]

    def process_frames_proxy(self, frames: List[FrameBlob], proxy_metadata: ProxyMetadata) -> List[FrameBlob]:
        confirmations = [self._process_frame_remotely_proxy(frame, proxy_metadata) for frame in frames]
        valid_confirmations = [(list(filter(lambda conf: conf is not None, confirmations)))]

        frames_count = len(frames)
        confirmations_count = len(valid_confirmations)

        if confirmations_count != frames_count:
            raise RuntimeError(
                f"Number of confirmations '{confirmations_count}' doesn't match frames count '{frames_count}'. "
                f"Frames may have been lost on the way."
            )

        response = copy(self.current_micro_session.frames_blobs_bucket)
        self.current_micro_session.frames_blobs_bucket.clear()
        return response


class MultiConnectionClient:
    def __init__(self, connection_managers: List[ConnectionManager] = None):
        self._connection_managers = connection_managers or []
        self._thread_pool_executor = ThreadPoolExecutor(thread_name_prefix="Hydra")

    @classmethod
    def from_urls(cls, nodes_urls: List[str]) -> MultiConnectionClient:
        connections = []
        for url in nodes_urls or ["http://127.0.0.1:2137"]:
            connection = ConnectionManager(url)
            connections.append(connection)
        return cls(connections)

    @property
    def connections(self) -> List[ConnectionManager]:
        return self._connection_managers

    def _add_node(self, node_url: str):
        connection_manager = ConnectionManager(node_url)
        self._connection_managers.append(connection_manager)

    def _get_frames_indexes_ranges_for_multi_send(self, frames_count: int) -> List[Tuple[int, int]]:
        frames_per_connection = frames_count // len(self._connection_managers)
        if frames_count % len(self._connection_managers) != 0:
            frames_per_connection += 1

        start_end_tuples = []

        # the last one will be shorter, so its end will be set to the end of batch manually to prevent IndexErrors
        for i in range(len(self._connection_managers) - 1):
            start_end_tuples.append((frames_per_connection * i, frames_per_connection * (i + 1)))
        start_end_tuples.append((start_end_tuples[-1][1], frames_count))

        return start_end_tuples

    def _ensure_all_connected(self):
        for manager in self._connection_managers:
            manager.sio.connect(manager.url)

    def _calculate_batch_bounds_per_worker(self, frames_count: int) -> Tuple[List[Tuple[int, int]], int]:
        bounds = (
            [(0, frames_count)]
            if len(self._connection_managers) == 1
            else self._get_frames_indexes_ranges_for_multi_send(frames_count)
        )

        number_of_connections = len(bounds)
        if number_of_connections != len(self._connection_managers):
            raise RuntimeError("Batches count doesn't match clients count.")

        return bounds, number_of_connections

    @staticmethod
    def _process_batch_remotely(
        manager: ConnectionManager,
        bounds: Tuple[int, int],
        frames: List[FrameBlob],
        proxy_metadata: Optional[ProxyMetadata] = None,
    ) -> List[FrameBlob]:
        manager.open_micro_session()
        res = (
            manager.process_frames_proxy(frames[bounds[0] : bounds[1]], proxy_metadata)
            if proxy_metadata
            else manager.process_frames(frames[bounds[0] : bounds[1]])
        )
        manager.close_micro_session()
        return res

    def get_processed_frame_blobs(
        self, frames: List[FrameBlob], proxy_metadata: Optional[ProxyMetadata] = None
    ) -> List[FrameBlob]:
        batches_bounds, connections_count = self._calculate_batch_bounds_per_worker(len(frames))
        self._ensure_all_connected()
        processed_frames: List[FrameBlob] = []

        # Submit tasks to different threads (every thread sends batch of input data to unique node)
        # Each thread fetches a batch of processed FrameBlobs from the peers (servers)
        processed_frames_lists_futures = [
            self._thread_pool_executor.submit(
                self._process_batch_remotely,
                self.connections[connection_id],
                batches_bounds[connection_id],
                frames,
                proxy_metadata,
            )
            for connection_id in range(connections_count)
        ]

        # Wait for all threads
        for future in processed_frames_lists_futures:
            processed_frames.extend(future.result())

        self.close_all_connections()
        return processed_frames

    def get_processed_bytes(self, data: bytes, proxy_metadata: Optional[ProxyMetadata]) -> bytes:
        # List of unprocessed FrameBlobs - every with 'RAW' status
        frames = stream_to_frames(data)

        # List of encoded UstreamChu/nks - wait until every frame will have 'ENCODED' status
        processed_frames = self.get_processed_frame_blobs(frames, proxy_metadata)

        return frames_to_stream(processed_frames)

    def close_all_connections(self):
        # Disconnect all clients
        for manager in self._connection_managers:
            manager.sio.disconnect()
