import datetime
import logging
from abc import abstractmethod
from types import FunctionType
from typing import List

from pyspark.sql import DataFrame

from dl_light_etl.errors import DataException
from dl_light_etl.types import AnyDataType, JobParameters

DEFAULT_KEY = "default_data_oject"

JOB_START_TIME = "job_start_time"
JOB_LAST_LOGGED_TIME = "job_last_logged_time"
JOB_DURATION = "job_duration"


class AbstractAction:
    """
    Abstract action
    Does not alter any data object or create any new ones
    May alter or create parameters
    Implementations should implement execute method with exact same parameters
    """

    @abstractmethod
    def execute(
        self, parameters: JobParameters, data: List[AnyDataType]
    ) -> JobParameters:
        pass


class DoNothingAction(AbstractAction):
    """Does nothing"""

    def execute(
        self, parameters: JobParameters, data: List[AnyDataType]
    ) -> JobParameters:
        return parameters


class LogTimeAction(AbstractAction):
    """Set time in the parameters with specified key"""

    def __init__(self, key: str):
        self.key = key

    def execute(
        self, parameters: JobParameters, data: List[AnyDataType]
    ) -> JobParameters:
        logging.info(f"Starting {type(self)}")
        parameters[self.key] = datetime.datetime.now()
        return parameters


class LogStartTimeAction(LogTimeAction):
    """Set the job start time"""

    def __init__(self):
        super().__init__(JOB_START_TIME)


class LogDurationAction(AbstractAction):
    """Logs the time and prints duration"""

    def execute(
        self, parameters: JobParameters, data: List[AnyDataType]
    ) -> JobParameters:
        logging.info(f"Starting {type(self)}")
        parameters[JOB_LAST_LOGGED_TIME] = datetime.datetime.now()
        parameters[JOB_DURATION] = (
            parameters[JOB_LAST_LOGGED_TIME] - parameters[JOB_START_TIME]
        ).total_seconds()
        logging.info(f"Job ran for {parameters[JOB_DURATION]} seconds")
        return parameters


class LogDataAction(AbstractAction):
    """Used for debugging purposes. Prints the contents"""

    def __init__(self, *, key: str = None, **print_params) -> None:
        self.key = key if key else DEFAULT_KEY
        self.print_params = print_params

    def execute(
        self, parameters: JobParameters, data: List[AnyDataType]
    ) -> JobParameters:
        logging.info(f"Starting {type(self)}")
        data = data[self.key]
        if type(data) == DataFrame:
            data: DataFrame = data
            logging.info(
                f"Showing DataFrame {self.key}:\n"
                + data._jdf.showString(
                    self.print_params.get("n", 20),
                    self.print_params.get("truncate", 20),
                    self.print_params.get("vertical", False),
                )
            )
        elif type(data) == list:
            data: List[str] = data
            n = self.print_params.get("n", 20)
            truncate = self.print_params.get("truncate", 20)
            for line in data[:n]:
                logging.info(line[:truncate])
        else:
            raise NotImplementedError(
                f"{type(self)} not implemented for data type {type(data)}"
            )
        return parameters


class SimpleDataValidationAction(DoNothingAction):
    """Validate one of the data objets"""

    def __init__(self, *, key: str, validation_fct: FunctionType) -> None:
        self.key = key if key else DEFAULT_KEY
        self.validation_fct = validation_fct

    def execute(
        self, parameters: JobParameters, data: List[AnyDataType]
    ) -> JobParameters:
        logging.info(f"Validating data in {type(self)}")
        self.validation_fct(data[self.key], parameters)
        return parameters


class RecordCountValidationAction(SimpleDataValidationAction):
    def __init__(self, *, key: str, expected_count: int) -> None:
        self.key = key if key else DEFAULT_KEY

        def validate_input_data(
            self, data: AnyDataType, parameters: JobParameters
        ) -> None:
            """Validate data on having n_lines hours"""
            logging.info(f"Starting {type(self)}")
            if type(data) == list:
                actual_count = len(data)
            elif type(data) == DataFrame:
                actual_count = data.count()
            else:
                raise NotImplementedError(
                    f"{type(self)} not implemented for data type {type(data)}"
                )

            if actual_count != expected_count:
                raise DataException(
                    f"DataFrame does not contain {expected_count} but {actual_count} records"
                )

        super().__init__(key=key, validation_fct=validate_input_data)
