import logging
from abc import abstractmethod
from datetime import date, datetime
from inspect import signature
from typing import Any, List, Optional
from types import FunctionType

from dl_light_etl.errors import ValidationException
from dl_light_etl.types import DateOrDatetime, DummyContext, EtlContext

DEFAULT_DATA_KEY = "final_df"
RUN_DATE = "dl_run_date"
RUN_TIME = "dl_run_time"
PROTECTED_KEYS = [RUN_DATE, RUN_TIME]


######################
# Generic constructs #
######################


class EtlStep:
    """Abstract class for an ETL action

    Implemented by either:
    - Extractor: read data
    - Transformer: apply transformation
    - Loader: save data
    - ValueGetter: get some value (to store in the etl_context.variables)
    - SideEffect: side-effect (e.g., log actions)

    :param default_input_aliases: For selecting the correct input parameters from the context.
    Should be empty list if execute() function takes no parameters
    :param default_output_alias: For putting the output parameter in the correct key in the context
    Should be set for Extractor, Transformer, ValueGetter
    Should be None for Loader, SideEffect
    """

    def __init__(
        self,
        default_input_aliases: List[str] = [],
        default_output_alias: Optional[str] = None,
    ) -> None:
        self._input_aliases = default_input_aliases
        self._output_alias = default_output_alias

    @property
    def has_output(self) -> bool:
        return self._output_alias is not None

    @abstractmethod
    def _execute(self, *args) -> Any:
        """Provides the etl result

        Is called by the process() method
        Be very strict on the type annotations when implementating this method
        """
        pass

    def process(self, context: EtlContext) -> EtlContext:
        """Processes the result of the step into the EtlContext

        The input parameters for the _execute() method are gathered from the EtlContext
        """
        input_parameters = [context[alias] for alias in self._input_aliases]
        output = self._execute(*input_parameters)
        out_context = dict(context)  # Making shallow copy
        if self._output_alias:
            out_context[self._output_alias] = output
        return out_context

    def validate(self, context: DummyContext) -> DummyContext:
        """Validates if the passed types match the annotations of the execute() method"""
        self._validate_input_aliases()
        self._validate_output_alias()
        sig = signature(self._execute)
        for parameter_alias, parameter in zip(
            self._input_aliases, sig.parameters.values()
        ):
            parameter_type = parameter.annotation
            logging.info(
                f"Checking for parameter {parameter_alias} of type {parameter_type}"
            )
            if parameter_alias not in context:
                raise ValidationException(
                    f"Key {parameter_alias} was not found in EtlContext"
                )
            if context[parameter_alias] != parameter_type:
                raise ValidationException(
                    f"Key {parameter_alias} in EtlContext is of type {context[parameter_alias]}, but {parameter_type} required"
                )

        out_context = dict(context)  # Making shallow copy
        if self.has_output:
            out_context[self._output_alias] = sig.return_annotation

        return out_context

    def on_alias(self, alias: str) -> "EtlStep":
        """Define the input parameter alias

        The corresponding value will be passed to the execute() method at runtime
        """
        return self.on_aliases(alias)

    def on_aliases(self, *alias: str) -> "EtlStep":
        """Define the input parameter aliases

        The corresponding values will be passed to the execute() method at runtime, in the given order
        """
        self._input_aliases = list(alias)
        return self

    def _validate_input_aliases(self) -> None:
        expected_n_params = len(signature(self._execute).parameters.values())
        actual_n_params = len(self._input_aliases)
        if not expected_n_params == actual_n_params:
            raise ValidationException(
                f"EtlStep {type(self)} expects {expected_n_params} input aliases, but {actual_n_params} given"
            )

    def alias(self, alias: str) -> "EtlStep":
        """Define the output alias"""
        self._output_alias = alias
        return self

    def _validate_output_alias(self) -> None:
        expected_retrun_type = signature(self._execute).return_annotation
        if not expected_retrun_type and self._output_alias:
            raise ValidationException(
                f"EtlStep {type(self)} has no output, but an alias was set"
            )
        if expected_retrun_type and (not self._output_alias):
            raise ValidationException(
                f"EtlStep {type(self)} has output, but no alias was set"
            )


class CompositeEtlStep(EtlStep):
    """Wrapper for several EtlSteps bundled together

    Executes the steps sequentially, passing an EtlContext, and gathering the results into the EtlContext
    In addition, can perform a "dummy run" using validate(); not passing actual data, but just the data types
    """

    def __init__(
        self,
        *steps: EtlStep,
        default_input_aliases: List[str] = [],
        default_output_alias: Optional[str] = None,
    ) -> None:
        super().__init__(default_input_aliases, default_output_alias)
        self._inner_input_aliases = default_input_aliases
        self._inner_output_alias = default_output_alias
        self.etl_steps = list(steps)

    def _execute(self, *args) -> Any:
        """Provides the etl result"""
        context: EtlContext = {
            alias: arg for alias, arg in zip(self._inner_input_aliases, args)
        }  # Making shallow copy

        for step in self.etl_steps:
            logging.info(f"Starting step {type(step)}")
            context = step.process(context)

        if self._output_alias:
            return context[self._inner_output_alias]

    def _validate_input_aliases(self):
        expected_n_params = len(self._inner_input_aliases)
        actual_n_params = len(self._input_aliases)
        if not expected_n_params == actual_n_params:
            raise ValidationException(
                f"EtlStep {type(self)} expects {expected_n_params} input aliases, but {actual_n_params} given"
            )

    def _validate_output_alias(self):
        if self._output_alias and (not self._inner_output_alias):
            raise ValidationException(
                f"EtlStep {type(self)} has no output, but an alias was set"
            )
        if not self._output_alias and self._inner_output_alias:
            raise ValidationException(
                f"EtlStep {type(self)} has output, but no alias was set"
            )

    def validate(self, context: DummyContext) -> DummyContext:
        """Checks if the correct aliases are passed around for all steps

        The dummy_context parameter contains the actual EtlContext keys, but instead of values, it contains the value type
        """
        logging.info(f"Validating step {type(self)}")
        new_context = {alias: context[alias] for alias in self._input_aliases}

        for step in self.etl_steps:
            logging.info(f"Validating step {type(step)}")
            new_context = step.validate(new_context)

        output_context = dict(context)  # Making shallow copy
        if self._output_alias:
            output_context[self._output_alias] = new_context[self._output_alias]
            return output_context


class EtlJob(CompositeEtlStep):
    """Etl Job; has not input other than the run date, and no outputs"""

    def __init__(self, run_date_or_time: DateOrDatetime, *steps: EtlStep) -> None:
        super().__init__(*steps)

        self.context: EtlContext = {}
        if type(run_date_or_time) == date:
            self.context[RUN_DATE] = run_date_or_time
        else:
            assert type(run_date_or_time) == datetime
            self.context[RUN_TIME] = run_date_or_time

    def process(self) -> None:
        self.validate()
        logging.info("Starting job")
        super().process(self.context)
        logging.info("Job completed")

    def validate(self) -> None:
        logging.info("Validating job")
        dummy_context: DummyContext = {
            key: type(val) for key, val in self.context.items()
        }
        super().validate(dummy_context)
        logging.info("Validation ok")


#####################
# Abstract EtlSteps #
#####################


class AbstractTransformer(EtlStep):
    """Abstract class for generic data transformer"""

    def __init__(self) -> None:
        super().__init__(
            default_input_aliases=[DEFAULT_DATA_KEY],
            default_output_alias=DEFAULT_DATA_KEY,
        )

    @abstractmethod
    def _execute(self, **kwargs) -> Any:
        pass


class AbstractExtractor(EtlStep):
    """Abstract class for generic extractor"""

    def __init__(self) -> None:
        super().__init__(default_output_alias=DEFAULT_DATA_KEY)

    @abstractmethod
    def _execute(self, **kwargs) -> Any:
        pass


class AbstractLoader(EtlStep):
    """Abstract class for saving a data object"""

    def __init__(self) -> None:
        super().__init__(default_input_aliases=[DEFAULT_DATA_KEY])

    @abstractmethod
    def _execute(self, **kwargs) -> None:
        pass


class AbstractSideEffect(EtlStep):
    """Abstract class for geeneric side effect

    Does not alter the etl context
    Intended for log actions, etc.
    """

    def __init__(self, default_input_aliases: List[str]) -> None:
        super().__init__(default_input_aliases)

    @abstractmethod
    def _execute(self, *args) -> None:
        pass


class AbstractValueGetter(EtlStep):
    """Abstract class for producing some value"""

    @abstractmethod
    def _execute(self, *args) -> Any:
        pass


###########################
# Generic implementations #
###########################


class FunctionExtractor(AbstractExtractor):
    """Extractor that simply executes a given function

    Can be wrapped around, e.g., http API services

    :param FunctionType extraction_fct: The function to execute
    :param **fct_params: The parameters to pass to the function
    """

    def __init__(self, extraction_fct: FunctionType, **fct_params) -> None:
        super().__init__()
        self.extraction_fct = extraction_fct
        self.fct_params = fct_params

    def _execute(self) -> Any:
        """Extract the data

        :returns: The extracted data
        :rtype: Any
        """
        data = self.extraction_fct(**self.fct_params)
        return data


class SimpleDataValidationSideEffect(AbstractSideEffect):
    """Validate one of the data objets"""

    def __init__(
        self,
        validation_fct: FunctionType,
        default_input_aliases: List[str] = [DEFAULT_DATA_KEY],
    ) -> None:
        super().__init__(default_input_aliases=default_input_aliases)
        self.validation_fct = validation_fct

    def _execute(self, *args) -> None:
        self.validation_fct(*args)


JOB_START_TIME = "job_start_time"


class JobStartTimeGetter(AbstractValueGetter):
    """Save the current time as the job_start_time"""

    def __init__(self) -> None:
        super().__init__(default_output_alias=JOB_START_TIME)

    def _execute(self) -> datetime:
        return datetime.now()


class LogDurationSideEffect(AbstractSideEffect):
    """Logs the duration since GetStartTime"""

    def __init__(self) -> None:
        super().__init__(default_input_aliases=[JOB_START_TIME])

    def _execute(self, job_start_time: datetime) -> None:
        duration = (datetime.now() - job_start_time).total_seconds()
        logging.info(f"Job ran for {duration} seconds")
