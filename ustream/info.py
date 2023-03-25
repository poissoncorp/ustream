from __future__ import annotations
from typing import List, Dict


class ProxyMetadata:
    def __init__(self, destination_url: str, path: List[str], hops_left: int = 1):
        self.destination_url = destination_url
        self.hops_left = hops_left
        self.path = path

    @classmethod
    def from_json(cls, json_dict: Dict) -> ProxyMetadata:
        return cls(json_dict["DestinationUrl"], json_dict["Path"], json_dict["HopsLeft"])

    def to_json(self) -> Dict:
        return {
            "DestinationUrl": self.destination_url,
            "Path": self.path,
            "HopsLeft": self.hops_left,
        }


class DeliveryConfirmation:
    def __init__(self, frame_id: int):
        self.frame_id = frame_id

    @classmethod
    def from_json(cls, json_dict: Dict) -> DeliveryConfirmation:
        return cls(json_dict["FrameID"])

    def to_json(self):
        return {"FrameId": self.frame_id}
