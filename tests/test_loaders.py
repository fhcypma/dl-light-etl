from pathlib import Path

from pyspark.sql import Row, SparkSession

from dl_light_etl.loaders import ParquetLoader, TextFileLoader
from dl_light_etl.types import StringRecords


def test_text_file_loader(rand_path: Path):
    # Given an etl context with a list of strings
    input_data: StringRecords = ["hello", "world"]
    context = {"data": input_data}
    # And a loader to save the data
    loader = TextFileLoader(rand_path).on_alias("data")
    # When the loader is processed
    loader.process(context)
    # Then the data should be written to the file
    output_data = rand_path.read_text()
    assert output_data == "\n".join(input_data)


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
