from __future__ import annotations
import random
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from typing import List, Tuple

import socketio

from chunks import UstreamChunk, chop_bytes_into_chunks, chunks_to_bytes

node_urls: List[str] = []


class SingleSocketSession:
    def __init__(self, sid: str):
        self.sid = sid


class SingleSocketClient:
    def __init__(self):
        self.sio = socketio.Client()

        @self.sio.event
        def connect():
            print(f"Connected with {self.sio.connection_url}")

        @self.sio.on("session_create")
        def my_message(data):
            print(f"Message received with {data}")

        @self.sio.on("*")
        def catch_all(event, data):
            print(event)
            print(data)

        @self.sio.on("session_close")
        def disconnect():
            print(f"Disconnected with {self.sio.connection_url}")

    def create_session(self):
        self.sio.emit("session_create", callback=print)

    def close_session(self):
        self.sio.emit("session_close", callback=print)

    def _send_chunk_to_server(self, data_chunk: UstreamChunk) -> UstreamChunk:
        print(f"Sending data: '{data_chunk.to_json()}'")
        result = []
        e = Event()

        def __set_value(val):
            result.append(val)
            e.set()

        self.sio.emit(
            "session_process",
            data_chunk.to_json(),
            callback=__set_value,
        )

        e.wait(5)
        return UstreamChunk.from_json(result.pop())

    def process_chunks(self, chunks: List[UstreamChunk]) -> List[UstreamChunk]:
        return [self._send_chunk_to_server(chunk) for chunk in chunks]


class MultiConnectionClient:
    def __init__(self, single_socket_clients: List[SingleSocketClient] = None):
        self.single_socket_clients = single_socket_clients or []
        self._thread_pool_executor = ThreadPoolExecutor(thread_name_prefix="Hydra")

    @classmethod
    def from_urls(cls, hosts_urls: List[str]) -> MultiConnectionClient:
        clients = []
        for url in hosts_urls:
            client = SingleSocketClient()
            client.sio.connect(url)
            clients.append(client)
        return cls(clients)

    def _get_chunks_indexes_ranges_for_multi_send(self, chunks_count: int) -> List[Tuple[int, int]]:
        chunks_per_client = chunks_count // len(self.single_socket_clients)
        if chunks_count % len(self.single_socket_clients) != 0:
            chunks_per_client += 1

        start_end_tuples = []

        # the last one will be shorter, so its end will be set to the end of batch manually to prevent IndexErrors
        for i in range(len(self.single_socket_clients) - 1):
            start_end_tuples.append((chunks_per_client * i, chunks_per_client * (i + 1)))
        start_end_tuples.append((start_end_tuples[-1][1], chunks_count))

        return start_end_tuples

    def split_chunks_into_batches_and_process_them_on_many_nodes_async(
        self, chunks: List[UstreamChunk]
    ) -> List[UstreamChunk]:
        # To skip batches allocation time, it'll only read "chunks" part by part
        bounds = self._get_chunks_indexes_ranges_for_multi_send(len(chunks))

        # Check if splitting went correctly and all data is going to be processed
        if len(bounds) != len(self.single_socket_clients):
            raise RuntimeError("Batches count doesn't match clients count.")

        results: List[UstreamChunk] = []

        # Submit tasks to different threads (every thread sends batch of input data to unique node)
        # Each thread fetches a batch of processed UstreamChunks from the peers (servers)
        futures = [
            self._thread_pool_executor.submit(
                lambda: self.single_socket_clients[i].process_chunks(chunks[bounds[i][0] : bounds[i][1]])
            )
            for i in range(len(bounds))
        ]

        # Wait for all threads
        for future in futures:
            results.extend(future.result(timeout=5))

        return results

    def get_processed_bytes(self, data: bytes) -> bytes:
        # List of unprocessed UstreamChunks - every with 'RAW' status
        chunks = chop_bytes_into_chunks(data)

        # List of encoded UstreamChunks - wait until every chunk will have 'ENCODED' status
        chunks = self.split_chunks_into_batches_and_process_them_on_many_nodes_async(chunks)

        return chunks_to_bytes(chunks)


def test_hydra(hosts_addresses: List[str] = None):
    if not hosts_addresses:
        hosts_addresses = ["http://127.0.0.1:2137"]
    hydra_client = MultiConnectionClient.from_urls(hosts_addresses)

    for i in range(10):
        length = random.randint(1, 10000)
        print(f"{i + 1} DATA SAMPLE with length of {length}")
        data = random.randbytes(length)
        result = hydra_client.get_processed_bytes(data)
        assert data == result
    print("Test successful!")


def collect_urls_from_file() -> List[str]:
    with open("topology.txt", "r") as file:
        return [f"http://{line}" for line in file.readlines()]


if __name__ == "__main__":
    node_urls.extend(collect_urls_from_file())
    test_hydra(node_urls)
