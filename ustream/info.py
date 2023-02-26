from __future__ import annotations
from typing import List, Dict


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
