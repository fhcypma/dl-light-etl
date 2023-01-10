import logging
from typing import List
from types import FunctionType

from pyspark.sql import DataFrame

from dl_light_etl.errors import DataException
from dl_light_etl.base import DEFAULT_DATA_KEY
from dl_light_etl.side_effects.abstract import AbstractSideEffect
from dl_light_etl.types import AnyDataType


class SimpleDataValidationSideEffect(AbstractSideEffect):
    """Validate one of the data objets"""

    def __init__(
        self,
        validation_fct: FunctionType,
        default_input_aliases: List[str] = [DEFAULT_DATA_KEY],
    ) -> None:
        super().__init__(default_input_aliases=default_input_aliases)
        self.validation_fct = validation_fct

    def _execute(self, *args) -> None:
        print(args)
        self.validation_fct(*args)


class RecordCountValidationSideEffect(SimpleDataValidationSideEffect):
    def __init__(self, expected_count: int) -> None:
        def validate_input_data(data_object: AnyDataType) -> None:
            """Validate data on having n_lines hours"""
            logging.info(f"Starting {type(self)}")
            if type(data_object) == list:
                actual_count = len(data_object)
            elif type(data_object) == DataFrame:
                actual_count = data_object.count()
            else:
                raise NotImplementedError(
                    f"{type(self)} not implemented for data type {type(data_object)}"
                )

            if actual_count != expected_count:
                raise DataException(
                    f"DataFrame does not contain {expected_count} but {actual_count} records"
                )

        super().__init__(validation_fct=validate_input_data)
