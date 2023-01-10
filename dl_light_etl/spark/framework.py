from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from dl_light_etl.base import (DEFAULT_DATA_KEY, JOB_START_TIME, RUN_DATE,
                               RUN_TIME, AbstractTransformer)
from dl_light_etl.types import DateOrDatetime


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
