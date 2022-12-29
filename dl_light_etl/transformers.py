import datetime
from abc import abstractmethod
from datetime import date, datetime
from typing import List, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from dl_light_etl.etl_constructs import (
    DEFAULT_DATA_KEY,
    RUN_DATE_KEY,
    RUN_TIME_KEY,
    EtlAction,
)
from dl_light_etl.side_effects.timing import JOB_START_TIME
from dl_light_etl.types import AnyDataType


class AbstractTransformer(EtlAction):
    """Abstract class for generic data transformer"""

    def __init__(self) -> None:
        super().__init__()
        self._has_output = True
        self._input_keys: List[str] = [DEFAULT_DATA_KEY]
        self._output_key = DEFAULT_DATA_KEY

    @abstractmethod
    def execute(self, **kwargs) -> AnyDataType:
        pass


class AddTechnicalFieldsTransformer(AbstractTransformer):
    """
    Adds following fields to (all) DataFrame(s):
    = dl_ingestion_time = start time of job
    - dl_input_file_name = pyspark.sql.functions.input_file_name()

    Requires a side_effects.timing.RememberStartTimeStep in the job
    """

    def __init__(self) -> None:
        super().__init__()
        self._input_keys = [DEFAULT_DATA_KEY, JOB_START_TIME]

    def execute(self, df: DataFrame, job_start_time: datetime) -> DataFrame:
        if type(df) != DataFrame:
            raise NotImplementedError(f"Cannot add field to data of type {type(df)}")

        df = df.withColumn("dl_ingestion_time", f.lit(job_start_time)).withColumn(
            "dl_input_file_name", f.input_file_name()
        )
        return df


class AddRunDateOrTimeTransformer(AbstractTransformer):
    """Adds dl_run_date or dl_run_time to the DataFrame"""

    def __init__(self, run_date_or_time: Union[date, datetime]) -> None:
        super().__init__()
        self._run_date_or_time = run_date_or_time

    def execute(self, df: DataFrame) -> DataFrame:
        if type(df) != DataFrame:
            raise NotImplementedError(f"Cannot add field to data of type {type(df)}")

        col_name = (
            RUN_DATE_KEY if type(self._run_date_or_time) == date else RUN_TIME_KEY
        )
        df = df.withColumn(col_name, f.lit(self._run_date_or_time))
        return df


class JoinTransformer(AbstractTransformer):
    """Utility transformer for joining two DataFrames"""

    def __init__(
        self, on: Union[str, List[str], Column, List[Column]], how: str = "inner"
    ) -> None:
        super().__init__()
        self.on = on
        self.how = how

    def execute(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        return df1.join(df2, self.on, self.how)
