from abc import ABC, abstractmethod


class AbstractSink(ABC):

    @abstractmethod
    def export():
        pass
