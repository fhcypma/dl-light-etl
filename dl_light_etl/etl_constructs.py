import logging
from abc import abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List
from inspect import signature

from dl_light_etl.types import DateOrDatetime, EtlContext
from dl_light_etl.utils.functional import fold_left
from dl_light_etl.errors import ValidationException

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

    def process(self, context: EtlContext) -> EtlContext:
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
    def dummy_process(self, dummy_context: Dict[str, type]) -> Dict[str, type]:
        """Pass around the context; validating if the required keys are there

        The dummy_context parameter contains the actual EtlContext keys, but instead of values, it contains the value type"""
        sig = signature(self.execute)
        for parameter_key, parameter in zip(self._input_keys, sig.parameters.values()):
            parameter_type = parameter.annotation
            logging.info(
                f"Checking for parameter {parameter_key} of type {parameter_type}"
            )
            if parameter_key not in dummy_context:
                raise ValidationException(
                    f"Key {parameter_key} was not found in EtlContext"
                )
            if dummy_context[parameter_key] != parameter_type:
                raise ValidationException(
                    f"Key {parameter_key} in EtlContext is of type {dummy_context[parameter_key]}, but {parameter_type} required"
                )

        if self._has_output:
            output_type = sig.return_annotation
            logging.info(f"Adding parameter {self._output_key} of type {output_type}")
            dummy_context[self._output_key] = output_type

        return dummy_context

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

    def execute(self) -> None:
        logging.info("Starting job")

        def execute_action(context: EtlContext, action: EtlAction) -> EtlContext:
            logging.info(f"Starting action {type(action)}")
            return action.process(context)

        fold_left(
            iterator=self.actions,
            accumulator=self.context,
            operator=execute_action,
        )
        logging.info("Job completed")

    def validate(self) -> None:
        logging.info("Validating job")

        def validate_action(
            dummy_context: Dict[str, type], action: EtlAction
        ) -> EtlContext:
            logging.info(f"Validating action {type(action)}")
            return action.dummy_process(dummy_context)

        fold_left(
            iterator=self.actions, accumulator=self.context, operator=validate_action
        )
        logging.info("Validation ok")
