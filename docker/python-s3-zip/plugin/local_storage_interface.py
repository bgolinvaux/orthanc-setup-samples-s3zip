from abc import ABC, abstractmethod
from contextlib import AbstractContextManager

class LocalStorageInterface(ABC):

    @abstractmethod
    def lease_folder(self, local_series_folder: str) -> AbstractContextManager[None]:
        pass

    @abstractmethod
    def folder_marker_critical_section(self, local_series_folder: str) -> AbstractContextManager[None]:
        pass

    @abstractmethod
    def write_file(self, local_series_folder: str, uuid: str, content: bytes):
        pass

    @abstractmethod
    def read_file(self, local_series_folder: str, uuid: str) -> bytes:
        pass
