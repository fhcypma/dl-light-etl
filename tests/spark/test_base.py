from pathlib import Path
import pytest
from pytest import LogCaptureFixture

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

from dl_light_etl.spark.base import *


##############
# Extractors #
##############


def test_text_extractor(spark_session: SparkSession, rand_path: Path):
    # Given a text file
    schema = StructType(
        [
            StructField("line", StringType()),
        ]
    )
    input_data = ["line1", "line2"]
    rand_path.write_text("\n".join(input_data))
    # When the file is read
    extractor = TextExtractor(input_path=rand_path, schema=schema).alias("out")
    output_context = extractor.process({})
    # Then the data should match the input data
    assert output_context["out"].collect() == [Row(line=line) for line in input_data]


def test_csv_extractor(spark_session: SparkSession, rand_path: Path):
    # Given a csv file
    schema = StructType(
        [
            StructField("col1", StringType()),
            StructField("col2", StringType()),
        ]
    )
    sep = "|"
    input_data = [["val1", "val2"], ["val1", "val2"]]
    input_lines = ["|".join(line) for line in input_data]
    rand_path.write_text("\n".join(input_lines))
    # And an extractor to read it
    extractor = CsvExtractor(input_path=rand_path, schema=schema, sep=sep).alias("out")
    # When extractor is processed
    output_context = extractor.process({})
    # Then the data should match the input data
    assert output_context["out"].collect() == [
        Row(col1="val1", col2="val2"),
        Row(col1="val1", col2="val2"),
    ]


def test_csv_extractor_validation(spark_session: SparkSession, rand_path: Path):
    # Given a csv file
    schema = StructType(
        [
            StructField("col1", StringType()),
            StructField("col2", StringType()),
        ]
    )
    sep = "|"
    input_data = [["val1", "val2"], ["val1", "val2"]]
    input_lines = ["|".join(line) for line in input_data]
    rand_path.write_text("\n".join(input_lines))
    # When the file is read
    extractor = CsvExtractor(input_path=rand_path, schema=schema, sep=sep)
    dummy_context = extractor.validate({})


################
# Transformers #
################


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


###########
# Loaders #
###########


def test_parquet_loader(spark_session: SparkSession, rand_path: Path):
    # Given an etl context with a DataFrame
    context = {
        "data": spark_session.createDataFrame([("val1", "val2")], ["col1", "col2"])
    }
    # And a loader to save the data to parquet
    loader = ParquetLoader(mode="overwrite", output_path=rand_path).on_alias("data")
    # When the loader is processed
    loader.process(context)
    # Then the data should be written to parquet
    output_df = spark_session.read.parquet(str(rand_path.resolve()))
    assert output_df.collect() == [Row(col1="val1", col2="val2")]


################
# Side Effects #
################


def test_get_or_create_spark_session(spark_session: SparkSession):
    # Given an etl context with spark config settings
    context = {SPARK_CONFIG: {
        "spark.app.name": "TestApp"
    }}
    # And an etl step to get or create the spark session
    step = GetOrCreateSparkSession()
    # When the step is processed
    step.process(context)
    # Then the spark session should have the configuration applied
    assert spark_session.conf.get("spark.app.name") == "TestApp"


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
    assert "|val|" in caplog.text
