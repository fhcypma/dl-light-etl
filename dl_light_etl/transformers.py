from abc import abstractmethod
from datetime import date, datetime
from typing import List, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from dl_light_etl.base import DEFAULT_DATA_KEY, RUN_DATE, RUN_TIME, EtlStep
from dl_light_etl.side_effects.timing import JOB_START_TIME
from dl_light_etl.types import AnyDataType, DateOrDatetime


class AbstractTransformer(EtlStep):
    """Abstract class for generic data transformer"""

    def __init__(self) -> None:
        super().__init__(
            default_input_aliases=[DEFAULT_DATA_KEY],
            default_output_alias=DEFAULT_DATA_KEY,
        )

    @abstractmethod
    def _execute(self, **kwargs) -> AnyDataType:
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

    def _execute(self, df: DataFrame, job_start_time: datetime) -> DataFrame:
        df = df.withColumn("dl_ingestion_time", f.lit(job_start_time)).withColumn(
            "dl_input_file_name", f.input_file_name()
        )
        return df


class AddRunDateOrTimeTransformer(AbstractTransformer):
    """Adds dl_run_date or dl_run_time to the DataFrame"""

    def _execute(self, df: DataFrame, run_date_or_time: DateOrDatetime) -> DataFrame:
        col_name = RUN_DATE if type(run_date_or_time) == date else RUN_TIME
        df = df.withColumn(col_name, f.lit(run_date_or_time))
        return df


class JoinTransformer(AbstractTransformer):
    """Utility transformer for joining two DataFrames"""

    def __init__(
        self, on: Union[str, List[str], Column, List[Column]], how: str = "inner"
    ) -> None:
        super().__init__()
        self.on = on
        self.how = how

    def _execute(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        return df1.alias(self._input_aliases[0]).join(
            df2.alias(self._input_aliases[1]), self.on, self.how
        )


class SelectTransformer(AbstractTransformer):
    """Select (a) column(s)"""

    def __init__(self, *cols: Union[str, Column]) -> None:
        super().__init__()
        self.cols = cols

    def _execute(self, df: DataFrame) -> DataFrame:
        return df.select(*self.cols)


class FilterTransformer(AbstractTransformer):
    """Filter on (a) column(s)"""

    def __init__(self, condition: Union[Column, str]) -> None:
        super().__init__()
        self.condition = condition

    def _execute(self, df: DataFrame) -> DataFrame:
        return df.filter(self.condition)
