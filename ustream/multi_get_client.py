from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import List, Tuple

import socketio

from ustream.chunks import UstreamChunk, chop_bytes_into_chunks, chunks_to_bytes



class SingleSocketClient:
    def __init__(self, url: str):
        self.sio = socketio.Client()
        self.url = url

        @self.sio.event
        def connect():
            print(f"[ {self.url} ] Connected with {self.sio.connection_url}")

        @self.sio.event
        def disconnect():
            print(f"[ {self.url} ] Disconnected with {self.url}")


        @self.sio.on("*")
        def catch_all(event, data):
            print(event)
            print(data)

    def _send_chunk_to_server(self, data_chunk: UstreamChunk) -> UstreamChunk:
        print(f"[ {self.url} ] Sending data: '{data_chunk.to_json()}'")
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
            client = SingleSocketClient(url)
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
        bounds = (
            [(0, len(chunks))]
            if len(self.single_socket_clients) == 1
            else self._get_chunks_indexes_ranges_for_multi_send(len(chunks))
        )

        # Check if splitting went correctly and all data is going to be processed
        if len(bounds) != len(self.single_socket_clients):
            raise RuntimeError("Batches count doesn't match clients count.")

        results: List[UstreamChunk] = []
        # Connect all clients
        for client in self.single_socket_clients:
            client.sio.connect(client.url)

        # Submit tasks to different threads (every thread sends batch of input data to unique node)
        # Each thread fetches a batch of processed UstreamChunks from the peers (servers)

        def _inspect(i):
            res = self.single_socket_clients[i].process_chunks(chunks[bounds[i][0] : bounds[i][1]])
            return res

        futures = [self._thread_pool_executor.submit(lambda i: _inspect(i), i) for i in range(len(bounds))]

        # Wait for all threads
        for future in futures:
            results.extend(future.result(timeout=5))

        # Disconnect all clients
        for client in self.single_socket_clients:
            client.sio.disconnect()

        return results

    def get_processed_bytes(self, data: bytes) -> bytes:
        # List of unprocessed UstreamChunks - every with 'RAW' status
        chunks = chop_bytes_into_chunks(data)

        # List of encoded UstreamChunks - wait until every chunk will have 'ENCODED' status
        chunks = self.split_chunks_into_batches_and_process_them_on_many_nodes_async(chunks)

        return chunks_to_bytes(chunks)
