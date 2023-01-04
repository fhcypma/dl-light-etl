import logging
from typing import List

from pyspark.sql import DataFrame

from dl_light_etl.etl_constructs import DEFAULT_DATA_KEY
from dl_light_etl.side_effects.abstract import AbstractSideEffect
from dl_light_etl.types import AnyDataType


class LogDataSideEffect(AbstractSideEffect):
    """Prints the contents of a given data_object

    Used for debugging purposes
    """

    def __init__(self, n: int = 20, truncate: int = 20, vertical: bool = False) -> None:
        super().__init__()
        self._input_keys = [DEFAULT_DATA_KEY]
        self.n = n
        self.truncate = truncate
        self.vertical = vertical

    def execute(self, data: AnyDataType) -> None:
        logging.info(f"Showing data object {self._input_keys[0]}:")
        if type(data) == DataFrame:
            data: DataFrame = data
            logging.info(
                "\n" + data._jdf.showString(self.n, self.truncate, self.vertical)
            )
        elif type(data) == list:
            data: List[str] = data
            for line in data[: self.n]:
                logging.info(line[: self.truncate])
        else:
            raise NotImplementedError(
                f"{type(self)} not implemented for data type {type(data)}"
            )
