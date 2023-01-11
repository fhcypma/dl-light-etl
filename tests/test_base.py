import pytest
from pytest import LogCaptureFixture
from datetime import date
import logging

from dl_light_etl.base import *
from dl_light_etl.types import EtlContext, DummyContext
from dl_light_etl.errors import ValidationException


class IntGenerator(EtlStep):
    """Simple EtlStep that generates an int"""

    def __init__(self, in_int: int) -> None:
        super().__init__(default_output_alias="out")
        self.in_int = in_int

    def _execute(self) -> int:
        return self.in_int


class LogIntTransformer(EtlStep):
    """Simple EtlStep that writes the int to the log"""

    def __init__(self) -> None:
        super().__init__(default_input_aliases=["in"])

    def _execute(self, in_int: int) -> None:
        return logging.info(f"Int is: {str(in_int)}")


class AddTwoIntsTransformer(EtlStep):
    """Simple EtlStep that adds two integers"""

    def __init__(self) -> None:
        super().__init__(default_input_aliases=["a", "b"], default_output_alias="out")

    def _execute(self, left: int, right: int) -> int:
        return left + right


class AddThreeIntsTransformer(CompositeEtlStep):
    """Sinple EtlStep that adds 3 integers"""

    # Using weird input names to test the inner workings
    def __init__(self) -> None:
        super().__init__(
            AddTwoIntsTransformer().on_aliases("a", "b").alias("intermediate"),
            AddTwoIntsTransformer().on_aliases("intermediate", "c").alias("out"),
            default_input_aliases=["a", "b", "c"],
            default_output_alias="out",
        )


class DoNothingStep(EtlStep):
    """Simple EtlStep that does really nothing"""

    def __init__(self) -> None:
        super().__init__(default_input_aliases=[], default_output_alias=None)

    def _execute(self) -> None:
        pass


def test_etl_step_process():
    # Given an etl step
    step = AddTwoIntsTransformer()
    # And an etl context
    context: EtlContext = {"a": 1, "b": 2}
    # When the step is processed
    output_context = step.process(context)
    # Then the result should be added to the output context
    assert output_context == {"a": 1, "b": 2, "out": 3}
    # And the original context stays the same
    assert context == {"a": 1, "b": 2}


def test_etl_step_validate():
    # Given an etl step
    step = AddTwoIntsTransformer()
    # And a dummy context
    context: DummyContext = {"a": int, "b": int}
    # When the step is validated
    output_context = step.validate(context)
    # Then the result type should be added to the output context
    assert output_context == {"a": int, "b": int, "out": int}
    # And the original context stays the same
    assert context == {"a": int, "b": int}


def test_etl_step_validate_fail():
    # Given an etl step
    step = AddTwoIntsTransformer()
    # And a dummy context that does not match the _execute function
    context: DummyContext = {"a": int, "b": str}  # note the str type there
    # When the step is validated
    # Then an exception is raised
    with pytest.raises(ValidationException):
        step.validate(context)


def test_etl_step_has_output():
    # Given an etl step with output
    step = AddTwoIntsTransformer()
    # When the has_output runs
    has_output = step.has_output
    # Then it should be true
    assert has_output


def test_etl_step_has_no_output():
    # Given an etl step with no output
    step = DoNothingStep()
    # When the has_output runs
    has_output = step.has_output
    # Then it should be true
    assert not has_output


def test_etl_step_set_input_aliases():
    # Given an etl step with input
    step = AddTwoIntsTransformer()
    # When the input aliases are set
    step = step.on_aliases("c", "d")
    # Then they should be set
    assert step._input_aliases == ["c", "d"]


def test_etl_step_validate_fail_incorrect_number_input_aliases():
    # Given an etl step with too few input aliases
    step = AddTwoIntsTransformer().on_aliases("a")
    # And a dummy context
    context: DummyContext = {"a": int, "b": int}
    # When the step is validated
    # Then an exception should be raised
    with pytest.raises(ValidationException):
        step.validate(context)


def test_etl_step_set_input_aliases_fail_none_expected():
    # Given an etl step with too many input aliases
    step = DoNothingStep().on_aliases("a")
    # And a dummy context
    context: DummyContext = {}
    # When the step is validated
    # Then an exception should be raised
    with pytest.raises(ValidationException):
        step.validate(context)


def test_etl_step_output_alias():
    # Given an etl step with output
    step = AddTwoIntsTransformer()
    # When the output alias is set
    step = step.alias("x")
    # Then they should be set
    assert step._output_alias == "x"


def test_etl_step_output_alias_fail_none_set():
    # Given an etl step with output, but no output alias is set
    step = AddTwoIntsTransformer().alias(None)
    # And a dummy context
    context: DummyContext = {"a": int, "b": int}
    # When the step is validated
    # Then an exception should be raised
    with pytest.raises(ValidationException):
        step.validate(context)


def test_etl_step_output_alias_fail_none_expected():
    # Given an etl step with no output, but with an output alias
    step = DoNothingStep().alias("x")
    # And a dummy context
    context: DummyContext = {}
    # When the step is validated
    # Then an exception should be raised
    with pytest.raises(ValidationException):
        step.validate(context)


def test_composite_etl_step_process():
    # Given a composite etl step
    step = AddThreeIntsTransformer().on_aliases("foo", "bar", "baz").alias("qux")
    # And an etl context
    context: EtlContext = {"foo": 1, "bar": 2, "baz": 3}
    # When the step is processed
    output_context = step.process(context)
    # Then the result should be added to the output context
    assert output_context == {"foo": 1, "bar": 2, "baz": 3, "qux": 6}
    # And the original context stays the same
    assert context == {"foo": 1, "bar": 2, "baz": 3}


def test_composite_etl_step_validate():
    # Given a composite etl step
    step = AddThreeIntsTransformer()
    # And a dummy etl context
    context: EtlContext = {"a": int, "b": int, "c": int}
    # When the step is validated
    output_context = step.validate(context)
    # Then the result should be added to the output context
    assert output_context == {"a": int, "b": int, "c": int, "out": int}
    # And the original context stays the same
    assert context == {"a": int, "b": int, "c": int}


def test_etl_job_process(caplog: LogCaptureFixture):
    # Given a job
    job = EtlJob(
        date(2022, 1, 1),
        IntGenerator(1).alias("a"),
        IntGenerator(2).alias("b"),
        IntGenerator(3).alias("c"),
        AddThreeIntsTransformer(),
        LogIntTransformer().on_alias("out"),
    )
    # When the job is processed
    with caplog.at_level(logging.INFO):
        job.process()
    # Then that last transformer should have printed something to the log
    assert "Int is: 6" in caplog.text


def test_etl_job_validate(caplog: LogCaptureFixture):
    # Given a correct job
    job = EtlJob(
        date(2022, 1, 1),
        IntGenerator(1).alias("a"),
        IntGenerator(2).alias("b"),
        IntGenerator(3).alias("c"),
        AddThreeIntsTransformer(),
        LogIntTransformer().on_alias("out"),
    )
    # When the job is validated
    # Then no exception should be raised
    with caplog.at_level(logging.INFO):
        job.validate()
    # And the log should read all types
    assert "IntGenerator" in caplog.text
    assert "AddTwoIntsTransformer" in caplog.text
    assert "LogIntTransformer" in caplog.text
    # And the log should read all parameters
    assert "parameter a" in caplog.text
    assert "parameter b" in caplog.text
    assert "parameter c" in caplog.text
    assert "parameter out" in caplog.text


def test_etl_job_validate_fail():
    # Given a correct job
    job = EtlJob(
        date(2022, 1, 1),
        IntGenerator(1).alias("a"),
        IntGenerator(2).alias("wrong!"),
        AddTwoIntsTransformer(),
        LogIntTransformer().on_alias("out"),
    )
    # When the job is validated
    # Then an exception should be raised
    with pytest.raises(ValidationException) as e:
        job.validate()
        assert "b was not found in EtlContext" in str(e)


def test_function_extractor():
    # Given a function that returns a string
    def greet(addressee: str) -> List[str]:
        return ["hello", addressee]

    # And a context
    # When the function is wrapped in an extractor and the data is extracted
    extractor = FunctionExtractor(greet, addressee="world").alias("out")
    output_context = extractor.process({})
    # Then the data should come from the function
    assert output_context["out"] == ["hello", "world"]


def test_log_time_action():
    # Given the time
    tic = datetime.now()
    # And an empty context
    context = {}
    # And an etl step to set the job start time
    step = JobStartTimeGetter()
    # When the step is processed
    out_context = step.process(context)
    # Then the start time should be set
    toc = datetime.now()
    assert JOB_START_TIME in out_context
    assert tic <= out_context[JOB_START_TIME]
    assert out_context[JOB_START_TIME] <= toc


def test_log_duration_side_effect(caplog: LogCaptureFixture):
    # Given an etl context with a start time
    start_time = datetime.now()
    etl_context = {JOB_START_TIME: start_time}
    # And an etl step to log the start time
    step = LogDurationSideEffect()
    # When step is processed
    with caplog.at_level(logging.INFO):
        output_parameters = step.process(etl_context)
    # Then the log should hold the duration
    assert f"Job ran for " in caplog.text
    assert f" seconds" in caplog.text
