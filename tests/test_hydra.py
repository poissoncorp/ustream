import random
from typing import List, Optional
from ustream.client import MultiConnectionClient
from ustream.constants import default_res_x, default_res_y, default_color_depth_bytes
from ustream.info import ProxyMetadata

node_urls: List[str] = []


def test_hydra(hosts_addresses: List[str] = None, test_count: int = 1, proxy_metadata: Optional[ProxyMetadata] = None):
    if not hosts_addresses:
        hosts_addresses = ["http://127.0.0.1:2137"]
    hydra_client = MultiConnectionClient.from_urls(hosts_addresses)
    try:
        for i in range(test_count):
            frames_count = random.randint(1, 60)
            length = default_res_x * default_res_y * default_color_depth_bytes * frames_count
            print(
                f"Processing {frames_count} frames of raw {default_res_x}x{default_res_y} {default_color_depth_bytes * 8}-bit color depth video."
            )
            print(f"{i + 1} DATA SAMPLE with length of {length}")
            data = random.randbytes(length)
            result = hydra_client.get_processed_bytes(data, proxy_metadata)
            # assert data == result
            print(f"{i+1} success!")
        print("Test successful!")
    finally:
        hydra_client.close_all_connections()


def collect_urls_from_file() -> List[str]:
    with open("topology.txt", "r") as file:
        return [f"http://{line.rstrip()}" for line in file.readlines()]


def test_proxy():
    test_hydra(node_urls, 2, ProxyMetadata("http://127.0.0.1:2137", [], 1, False))


def test_ordinary():
    test_hydra(node_urls, 1)


if __name__ == "__main__":
    node_urls.extend(collect_urls_from_file())
    test_ordinary()
