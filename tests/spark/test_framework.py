from datetime import datetime

from pyspark.sql import SparkSession

from dl_light_etl.base import RUN_DATE, RUN_TIME
from dl_light_etl.spark.framework import *


def test_add_technical_fields_transformer(spark_session: SparkSession):
    # Given a time
    time = datetime.now()
    # And an etl context with a dataframe and a time
    context = {
        "data": spark_session.createDataFrame([("val1",), ("val2",)], ["value"]),
        "time": time,
    }
    # And a transformer
    transformer = (
        AddTechnicalFieldsTransformer().on_aliases("data", "time").alias("out")
    )
    # When the technical fields are added
    output_context = transformer.process(context)
    # Then the data should contain the technical columns
    assert output_context["out"].columns == [
        "value",
        "dl_ingestion_time",
        "dl_input_file_name",
    ]
    assert output_context["out"].collect()[0][1] == time


def test_add_run_date_or_time_transformer(spark_session: SparkSession):
    # Given a time
    time = datetime.now()
    # And an etl context with a dataframe and a time and a date
    context = {
        "data": spark_session.createDataFrame([("val1",), ("val2",)], ["value"]),
        "time": time,
        "date": time.date(),
    }
    # And a transformer that adds the run time
    transformer = AddRunDateOrTimeTransformer().on_aliases("data", "time").alias("out")
    # When the run time is added
    output_context = transformer.process(context)
    # Then the data should contain the time
    assert output_context["out"].columns == ["value", RUN_TIME]
    assert output_context["out"].collect()[0][1] == time

    # And when the transformer adds the run date
    transformer = transformer.on_aliases("data", "date")
    output_context = transformer.process(context)
    # Then the data should contain the date
    assert output_context["out"].columns == ["value", RUN_DATE]
    assert output_context["out"].collect()[0][1] == time.date()
