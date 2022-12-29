import logging

from pyspark.sql import SparkSession
from pytest import LogCaptureFixture

from dl_light_etl.etl_constructs import DEFAULT_DATA_KEY, EtlContext
from dl_light_etl.side_effects.data_logging import LogDataSideEffect
from dl_light_etl.types import StringRecords


def test_log_data_action_string(caplog: LogCaptureFixture):
    # Given lines of data
    input_data: StringRecords = ["line1", "line2"]
    # And a context containing that data
    key = "custom_key"
    context: EtlContext = {key: input_data}
    # When the data is logged
    action = LogDataSideEffect().with_input_keys(key)
    with caplog.at_level(logging.INFO):
        action.process(context)
    # Then the data should be in the logs
    for line in input_data:
        assert line in caplog.text


def test_log_data_action_string_limited_output(caplog: LogCaptureFixture):
    # Given lines of data
    input_data: StringRecords = ["line1", "line2"]

    context = {DEFAULT_DATA_KEY: input_data}
    # When the data is logged
    action = LogDataSideEffect(n=1)
    with caplog.at_level(logging.INFO):
        action.process(context)
    # Then the data should be in the logs
    assert input_data[0] in caplog.text
    assert not input_data[1] in caplog.text


def test_log_data_action_dataframe(
    caplog: LogCaptureFixture, spark_session: SparkSession
):
    # Given a dataframe
    input_data = spark_session.createDataFrame([["val"]], ["col"])
    context = {DEFAULT_DATA_KEY: input_data}
    # When the data is logged
    action = LogDataSideEffect()
    with caplog.at_level(logging.INFO):
        action.process(context)
    # Then the data should be in the logs
    print("logs:")
    print(caplog.text)
    assert "|val|" in caplog.text
