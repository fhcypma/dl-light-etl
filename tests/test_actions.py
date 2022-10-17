import datetime
import logging
import random
from pytest import LogCaptureFixture
from pyspark.sql import SparkSession

from dl_light_etl.actions import (
    DEFAULT_KEY,
    JOB_DURATION,
    JOB_LAST_LOGGED_TIME,
    JOB_START_TIME,
    DoNothingAction,
    LogDataAction,
    LogDurationAction,
    LogTimeAction,
)


def test_do_nothing_action():
    # Given a random value
    val = random.randint(0, 100)
    # When no action is performed
    action = DoNothingAction()
    par = action.execute(parameters={"val": val}, data=None)
    # Then the value stays the same
    assert par["val"] == val


def test_log_time_action():
    # Given the time
    tic = datetime.datetime.now()
    # When the job start time is set
    key = "time_key"
    action = LogTimeAction(key)
    output_parameters = action.execute(parameters={}, data=None)
    # Then the start time should be set
    toc = datetime.datetime.now()
    assert key in output_parameters
    assert tic <= output_parameters[key]
    assert output_parameters[key] <= toc


def test_log_duration_action(caplog):
    # Given the job start time
    input_parameters = {JOB_START_TIME: datetime.datetime.now()}
    # When the duration is logged
    action = LogDurationAction()
    with caplog.at_level(logging.INFO):
        output_parameters = action.execute(parameters=input_parameters, data=None)
    # Then the last logged time should be set
    assert JOB_LAST_LOGGED_TIME in output_parameters
    # And the duration should be set
    assert JOB_DURATION in output_parameters
    # And the log should hold the duration
    assert f"Job ran for {output_parameters[JOB_DURATION]} seconds" in caplog.text


def test_log_data_action_string(caplog: LogCaptureFixture):
    # Given lines of data
    input_data = ["line1", "line2"]
    # When the data is logged
    action = LogDataAction()
    with caplog.at_level(logging.INFO):
        action.execute(parameters={}, data={DEFAULT_KEY: input_data})
    # Then the data should be in the logs
    for line in input_data:
        assert line in caplog.text


def test_log_data_action_string_limited_output(caplog: LogCaptureFixture):
    # Given lines of data
    input_data = ["line1", "line2"]
    # When the data is logged
    action = LogDataAction(n=1)
    with caplog.at_level(logging.INFO):
        action.execute(parameters={}, data={DEFAULT_KEY: input_data})
    # Then the data should be in the logs
    assert input_data[0] in caplog.text
    assert not input_data[1] in caplog.text


def test_log_data_action_dataframe(
    caplog: LogCaptureFixture, spark_session: SparkSession
):
    # Given a dataframe
    input_data = spark_session.createDataFrame([["val"]], ["col"])
    # When the data is logged
    action = LogDataAction()
    with caplog.at_level(logging.INFO):
        action.execute(parameters={}, data={DEFAULT_KEY: input_data})
    # Then the data should be in the logs
    print("logs:")
    print(caplog.text)
    assert "|val|" in caplog.text
