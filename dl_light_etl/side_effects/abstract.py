from abc import abstractmethod
from typing import Any, List

from dl_light_etl.etl_constructs import EtlAction


class AbstractSideEffect(EtlAction):
    """Abstract class for geeneric side effect

    Does not alter the etl context
    Intended for log actions, etc.
    """
    
    def __init__(self) -> None:
        super().__init__()
        self._input_keys: List[str] = []
        self._output_key = None
        self._has_output = False


    @abstractmethod
    def execute(self, **kwargs) -> None:
        pass


class AbstractValueGetter(EtlAction):
    """Abstract class for producing some value"""
    
    def __init__(self) -> None:
        super().__init__()
        self._has_output = True
        # Values below to be set in implementation
        self._input_keys: List[str] = []
        self._output_key = None

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass