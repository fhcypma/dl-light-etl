from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from dl_light_etl.base import (
    DEFAULT_DATA_KEY,
    JOB_START_TIME,
    RUN_DATE,
    RUN_TIME,
    AbstractTransformer,
)


class AddTechnicalFieldsTransformer(AbstractTransformer):
    """
    Adds following fields to (all) DataFrame(s):
    = dl_ingestion_time = start time of job
    - dl_input_file_name = pyspark.sql.functions.input_file_name()

    Requires a side_effects.timing.RememberStartTimeStep in the job
    """

    def __init__(self) -> None:
        super().__init__()
        self._input_aliases = [DEFAULT_DATA_KEY, JOB_START_TIME]

    def _execute(self, df: DataFrame, job_start_time: datetime) -> DataFrame:
        df = df.withColumn("dl_ingestion_time", f.lit(job_start_time)).withColumn(
            "dl_input_file_name", f.input_file_name()
        )
        return df


class AddRunDateTransformer(AbstractTransformer):
    """Adds dl_run_date or dl_run_time to the DataFrame"""

    def __init__(self) -> None:
        super().__init__()
        self._input_aliases = [DEFAULT_DATA_KEY, RUN_DATE]

    def _execute(self, df: DataFrame, run_date: date) -> DataFrame:
        df = df.withColumn(RUN_DATE, f.lit(run_date))
        return df


class AddRunTimeTransformer(AbstractTransformer):
    """Adds dl_run_date or dl_run_time to the DataFrame"""

    def __init__(self) -> None:
        super().__init__()
        self._input_aliases = [DEFAULT_DATA_KEY, RUN_TIME]

    def _execute(self, df: DataFrame, run_time: datetime) -> DataFrame:
        df = df.withColumn(RUN_TIME, f.lit(run_time))
        return df
