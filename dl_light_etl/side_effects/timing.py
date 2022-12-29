from datetime import datetime
import logging

from dl_light_etl.side_effects.abstract import AbstractValueGetter, AbstractSideEffect
from dl_light_etl.etl_constructs import EtlContext

JOB_START_TIME = "job_start_time"


class JobStartTimeGetter(AbstractValueGetter):
    """Save the current time as the job_start_time"""
    
    def __init__(self) -> None:
        super().__init__()
        self._output_key = JOB_START_TIME
    
    def execute(self) -> datetime:
        return datetime.now()


class LogDurationSideEffect(AbstractSideEffect):
    """Logs the duration since GetStartTime"""
    
    def __init__(self) -> None:
        super().__init__()
        self._input_keys = [JOB_START_TIME]

    def execute(self, job_start_time: datetime) -> None:
        duration = (datetime.now() - job_start_time).total_seconds()
        logging.info(f"Job ran for {duration} seconds")