import logging
from abc import abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List

from dl_light_etl.types import DateOrDatetime, EtlContext
from dl_light_etl.utils.functional import fold_left

DEFAULT_DATA_KEY = "final_df"
RUN_DATE_KEY = "run_date"
RUN_TIME_KEY = "run_time"
PROTECTED_KEYS = [RUN_DATE_KEY, RUN_TIME_KEY]


class EtlAction:
    """Abstract class for an action

    Implemented by either:
    - Extractor: read data
    - Transformer: apply transformation
    - Loader: save data
    - ValueGetter: get some value (to store in the etl_context.variables)
    - Side-effect: side-effect (e.g., log actions)
    """

    def __init__(self) -> None:
        # Each implementation of this class should set own values for the following vars:
        # For selecting the correct input parameters from the context
        self._input_keys: List[str] = [DEFAULT_DATA_KEY]
        # For putting the output parameter in the correct key in the contet
        self._output_key = DEFAULT_DATA_KEY
        # Should be True for Extractor, Transformer, ValueGetter
        self._has_output = False

    def process(self, context: Dict[str, Any]) -> EtlContext:
        """Gets the correct parameters from context, executes actions and puts result back in context"""
        input_parameters = [context[key] for key in self._input_keys]
        output = self.execute(*input_parameters)
        if self._has_output:
            new_context = context.copy()
            new_context[self._output_key] = output
            return new_context
        else:
            return context

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass

    def with_input_keys(self, *keys: str) -> "EtlAction":
        self._input_keys = list(keys)
        return self

    def with_output_key(self, key: str) -> "EtlAction":
        self._output_key = key
        return self


class EtlJob:
    def __init__(
        self, *, run_date_or_time: DateOrDatetime, actions: List[EtlAction]
    ) -> None:
        self.context: EtlContext = {}
        if type(run_date_or_time) == date:
            self.context[RUN_DATE_KEY] = run_date_or_time
        else:
            assert type(run_date_or_time) == datetime
            self.context[RUN_TIME_KEY] = run_date_or_time

        self.actions: List[EtlAction] = actions
        # TODO validate actions?

    @staticmethod
    def _execute_action(context: EtlContext, action: EtlAction) -> EtlContext:
        logging.info(f"Starting action {type(action)}")
        return action.process(context)

    def execute(self) -> None:
        logging.info("Starting job")
        fold_left(
            iterator=self.actions,
            accumulator=self.context,
            operator=self._execute_action,
        )
        logging.info("Job completed")
