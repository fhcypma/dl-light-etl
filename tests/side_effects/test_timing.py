import datetime
import logging
from pytest import LogCaptureFixture

from dl_light_etl.side_effects.timing import (
    JOB_START_TIME,
    JobStartTimeGetter,
    LogDurationSideEffect,
)


def test_log_time_action():
    # Given the time
    tic = datetime.datetime.now()
    # And an empty context
    context = {}
    # And an etl step to set the job start time
    step = JobStartTimeGetter()
    # When the step is processed
    out_context = step.process(context)
    # Then the start time should be set
    toc = datetime.datetime.now()
    assert JOB_START_TIME in out_context
    assert tic <= out_context[JOB_START_TIME]
    assert out_context[JOB_START_TIME] <= toc


def test_log_duration_side_effect(caplog: LogCaptureFixture):
    # Given an etl context with a start time
    start_time = datetime.datetime.now()
    etl_context = {JOB_START_TIME: start_time}
    # And an etl step to log the start time
    step = LogDurationSideEffect()
    # When step is processed
    with caplog.at_level(logging.INFO):
        output_parameters = step.process(etl_context)
    # Then the log should hold the duration
    assert f"Job ran for " in caplog.text
    assert f" seconds" in caplog.text
