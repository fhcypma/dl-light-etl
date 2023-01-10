from abc import abstractmethod
from typing import Any, List

from dl_light_etl.base import EtlStep


class AbstractSideEffect(EtlStep):
    """Abstract class for geeneric side effect

    Does not alter the etl context
    Intended for log actions, etc.
    """

    def __init__(self, default_input_aliases: List[str]) -> None:
        super().__init__(default_input_aliases)

    @abstractmethod
    def _execute(self, *args) -> None:
        pass


class AbstractValueGetter(EtlStep):
    """Abstract class for producing some value"""

    @abstractmethod
    def _execute(self, *args) -> Any:
        pass
