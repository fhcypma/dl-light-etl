from pathlib import Path

from pyspark.sql import Row, SparkSession

from dl_light_etl.types import StringRecords
from dl_light_etl.etl_constructs import EtlContext, DEFAULT_DATA_KEY
from dl_light_etl.loaders import ParquetLoader, TextFileLoader


def test_text_file_loader(rand_path: Path):
    # Given a list of string lines
    input_data: StringRecords = ["hello", "world"]
    # When the data is loaded
    loader = TextFileLoader(rand_path)
    output_data = loader.execute(input_data)
    # Then the data should be written to the file
    output_data = rand_path.read_text()
    assert output_data == "\n".join(input_data)


def test_parquet_loader(spark_session: SparkSession, rand_path: Path):
    # Given an input data frame
    input_data = [("val1", "val2")]
    input_schema = ["col1", "col2"]
    input_df = spark_session.createDataFrame(input_data, input_schema)
    # When the data is loaded to parquet
    loader = ParquetLoader(mode="overwrite", output_path=rand_path)
    loader.execute(input_df)
    # Then the data should be written to parquet
    output_df = spark_session.read.parquet(str(rand_path.resolve()))
    assert output_df.collect() == [Row(col1="val1", col2="val2")]
