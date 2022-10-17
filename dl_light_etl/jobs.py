import datetime
import logging
from typing import List

from pyspark.sql import SparkSession

from dl_light_etl.actions import DoNothingAction, LogDurationAction, LogStartTimeAction
from dl_light_etl.extractors import Extractors
from dl_light_etl.loaders import AbstractLoader
from dl_light_etl.transformers import Transformers
from dl_light_etl.types import AnyDataType, JobParameters


class DailyJob:
    def __init__(self, run_date: datetime.date) -> None:
        assert type(run_date) == datetime.date

        self.run_date = run_date


class FrequentJob:
    def __init__(self, run_datetime: datetime.datetime) -> None:
        assert type(run_datetime) == datetime.datetime

        self.run_datetime = run_datetime


class SparkDrivenJob:
    def __init__(self) -> None:
        self.spark = SparkSession.Builder.getOrCreate()


class EtlJob:
    def __init__(
        self,
        *,
        pre_extractor_action: DoNothingAction = None,
        extractors: Extractors,
        post_extractor_action: DoNothingAction = None,
        transformers: Transformers = None,
        pre_loader_action: DoNothingAction = None,
        loader: AbstractLoader,
        post_loader_action: DoNothingAction = None,
    ) -> None:
        self.pre_extractor_action = (
            pre_extractor_action if pre_extractor_action else LogStartTimeAction()
        )

        assert type(extractors) == Extractors
        assert len(extractors.extractors) > 0, "No extractor defined for job"
        self.extractors = extractors

        self.post_extractor_action = (
            post_extractor_action if post_extractor_action else DoNothingAction()
        )

        assert not transformers or type(transformers) == Transformers
        self.transformer = transformers if transformers else Transformers()

        self.pre_loader_action = (
            pre_loader_action if pre_loader_action else DoNothingAction()
        )

        assert issubclass(type(loader), AbstractLoader)
        self.loader = loader

        self.post_loader_action = (
            post_loader_action if post_loader_action else LogDurationAction()
        )

    def execute(self) -> None:
        parameters: JobParameters = {}
        data: List[AnyDataType] = []
        logging.info("Phase: Extract")
        parameters = self.pre_extractor_action.execute(parameters, data)
        parameters, data = self.extractors.extract(parameters)
        parameters = self.post_extractor_action.execute(parameters, data)
        logging.info("Phase: Transform")
        parameters, data = self.transformer.transform(parameters, data)
        logging.info("Phase: Load")
        parameters = self.pre_loader_action.execute(parameters, data)
        parameters = self.loader.load(parameters, data)
        parameters = self.post_loader_action.execute(parameters, data)


class SparkJob(EtlJob):
    def __init__(
        self,
        *,
        pre_extractor_action: DoNothingAction = None,
        extractors: Extractors,
        post_extractor_action: DoNothingAction = None,
        transformers: Transformers = None,
        pre_loader_action: DoNothingAction = None,
        loader: AbstractLoader,
        post_loader_action: DoNothingAction = None,
    ) -> None:
        super().__init__(
            pre_extractor_action=pre_extractor_action,
            extractors=extractors,
            post_extractor_action=post_extractor_action,
            transformers=transformers,
            pre_loader_action=pre_loader_action,
            loader=loader,
            post_loader_action=post_loader_action,
        )
