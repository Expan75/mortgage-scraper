from pandas import DataFrame
from abc import ABC, abstractmethod


class AbstractSink(ABC):

    @abstractmethod
    def export(self, df: DataFrame, name: str):
        pass
