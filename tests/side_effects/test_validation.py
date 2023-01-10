import pytest

from pyspark.sql import SparkSession

from dl_light_etl.base import DEFAULT_DATA_KEY
from dl_light_etl.errors import DataException
from dl_light_etl.side_effects.validation import RecordCountValidationSideEffect


def test_record_count_validation(spark_session: SparkSession):
    # Given an etl context with a dataframe with 1 record
    context = {DEFAULT_DATA_KEY: spark_session.createDataFrame([["val"]], ["col"])}
    # And an etl step to validate that the record count is 1
    step = RecordCountValidationSideEffect(expected_count=1)
    # When the step is processed
    # Then no exception is raised
    step.process(context)


def test_record_count_validation_fail(spark_session: SparkSession):
    # Given an etl context with a dataframe with 1 record
    context = {DEFAULT_DATA_KEY: spark_session.createDataFrame([["val"]], ["col"])}
    # And an etl step to validate that the record count is 2
    step = RecordCountValidationSideEffect(expected_count=2)
    # When the step is processed
    # Then a DataException exception is raised
    with pytest.raises(DataException):
        step.process(context)
