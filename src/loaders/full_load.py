from abc import ABC, abstractmethod


class Loader(ABC):
    def __init__(self, **kwargs):
        print("Super")

    @abstractmethod
    def load(self):
        """Logic to handle the load"""
        return


class IncrementalLoader(Loader):
    def __init__(self, **kwargs):
        super().__init__(kwargs)

    def load(self):
        """Logic to handle the load"""
        return


class FullLoader(Loader):
    def __init__(self, **kwargs):
        super().__init__(kwargs)

    def load(self):
        """Logic to handle the load"""
        return
