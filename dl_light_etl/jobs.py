import logging
from typing import List, Optional

from dl_light_etl.actions import DoNothingAction, LogDurationAction, LogStartTimeAction
from dl_light_etl.extractors import Extractors
from dl_light_etl.loaders import AbstractLoader
from dl_light_etl.transformers import Transformers
from dl_light_etl.types import AnyDataType, JobParameters


class EtlJob:
    def __init__(
        self,
        *,
        pre_extractor_action: Optional[DoNothingAction] = None,
        extractors: Extractors,
        post_extractor_action: Optional[DoNothingAction] = None,
        transformers: Optional[Transformers] = None,
        pre_loader_action: Optional[DoNothingAction] = None,
        loader: AbstractLoader,
        post_loader_action: Optional[DoNothingAction] = None,
    ) -> None:
        self.pre_extractor_action = (
            pre_extractor_action if pre_extractor_action else LogStartTimeAction()
        )

        assert isinstance(extractors, Extractors)
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

        assert isinstance(loader, AbstractLoader)
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
