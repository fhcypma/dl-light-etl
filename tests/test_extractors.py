from pathlib import Path

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructField, StructType

from dl_light_etl.etl_constructs import EtlContext
from dl_light_etl.extractors import (
    CsvExtractor,
    FunctionExtractor,
    TextExtractor,
    TextFileExtractor,
)
from dl_light_etl.types import StringRecords


def test_simple_function_extractor():
    # Given a function that returns a string
    def greet(addressee: str) -> StringRecords:
        return ["hello", addressee]

    # And a context
    # When the function is wrapped in an extractor and the data is extracted
    extractor = FunctionExtractor(greet, addressee="world")
    output_data = extractor.execute()
    # Then the data should come from the function
    assert output_data == ["hello", "world"]


def test_text_file_extractor(rand_path: Path):
    # Given a text file
    input_data = ["This", "is", "the", "data"]
    rand_path.write_text("\n".join(input_data))
    # When the file is read
    extractor = TextFileExtractor(input_path=rand_path)
    output_data = extractor.execute()
    # Then the data should be read
    assert output_data == input_data


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
    extractor = TextExtractor(input_path=rand_path, schema=schema)
    output_data = extractor.execute()
    # Then the data should match the input data
    assert output_data.collect() == [Row(line=line) for line in input_data]


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
    # When the file is read
    extractor = CsvExtractor(input_path=rand_path, schema=schema, sep=sep)
    output_data = extractor.execute()
    # Then the data should match the input data
    assert output_data.collect() == [
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
    dummy_context = extractor.dummy_process({})
