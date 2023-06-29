from typing import Dict, Any
from abc import ABC, abstractmethod


class AbstractSink(ABC):

    @abstractmethod
    def __init__(self, namespace: str):
        pass

    @abstractmethod
    def write(self, data_record: Dict[str, Any]):
        pass

    @abstractmethod
    def close(self):
        pass
