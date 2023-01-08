import random
from typing import List
from ustream.client import MultiConnectionClient

node_urls: List[str] = []


def test_hydra(hosts_addresses: List[str] = None, test_count: int = 1):
    if not hosts_addresses:
        hosts_addresses = ["http://127.0.0.1:2137"]
    hydra_client = MultiConnectionClient.from_urls(hosts_addresses)
    try:
        for i in range(test_count):
            length = random.randint(1, 10000)
            print(f"{i + 1} DATA SAMPLE with length of {length}")
            data = random.randbytes(length)
            result = hydra_client.get_processed_bytes(data)
            assert data == result
            print(f"{i+1} success!")
        print("Test successful!")
    finally:
        hydra_client.close_all_connections()


def collect_urls_from_file() -> List[str]:
    with open("topology.txt", "r") as file:
        return [f"http://{line.rstrip()}" for line in file.readlines()]


if __name__ == "__main__":
    node_urls.extend(collect_urls_from_file())
    test_hydra(node_urls, 2)
