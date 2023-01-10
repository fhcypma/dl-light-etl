from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dl_light_etl.base import RUN_DATE, RUN_TIME
from dl_light_etl.transformers import (
    AddRunDateOrTimeTransformer,
    AddTechnicalFieldsTransformer,
    FilterTransformer,
    JoinTransformer,
    SelectTransformer,
)


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


def test_join_transformer(spark_session: SparkSession):
    # Given an etl context with two dataframes
    context = {
        "data1": spark_session.createDataFrame([(1, "a")], ["id", "val"]),
        "data2": spark_session.createDataFrame([(1, "A"), (2, "B")], ["id", "val"]),
    }
    # And a transformer to join the dataframes
    transformer = JoinTransformer("id").on_aliases("data1", "data2").alias("out")
    # When the transformer is processed
    output_context = transformer.process(context)
    # Then the data should be joined
    # And there should be aliases in place, based on the input keys
    output_context["out"].select("id", "data1.val", "data2.val").collect()[0] == [
        1,
        "a",
        "A",
    ]


def test_select_transformer(spark_session: SparkSession):
    # Given n etl context with a dataframe
    context = {"data": spark_session.createDataFrame([(1, "a")], ["id", "val"])}
    # And a transformer to select columns
    transformer = SelectTransformer("id").on_alias("data").alias("out")
    # When the transformer is processed
    output_context = transformer.process(context)
    # Then only that column should be selected
    assert output_context["out"].columns == ["id"]
    # And the data should remain unchanged
    assert output_context["out"].collect()[0][0] == 1


def test_filter_transformer(spark_session: SparkSession):
    # Given an etl context with a dataframe
    context = {
        "data": spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    }
    # And a transformer to filter the data
    transformer = FilterTransformer(col("id") == 1).on_alias("data").alias("out")
    # When records are filteres
    output_context = transformer.process(context)
    # Then the data shoulld be filtered
    assert output_context["out"].count() == 1
