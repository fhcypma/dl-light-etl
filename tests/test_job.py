import logging
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
from pytest import LogCaptureFixture

from dl_light_etl.base import (
    RUN_DATE,
    JOB_START_TIME,
    TimedEtlJob,
    EtlJob,
    FunctionExtractor,
    LogDurationSideEffect,
    SetJobStartTime,
)
from dl_light_etl.errors import ValidationException
from dl_light_etl.plain_text.base import TextFileLoader
from dl_light_etl.spark.base import *
from dl_light_etl.spark.framework import AddTechnicalFieldsTransformer
from dl_light_etl.types import StringRecords


def test_csv_join_to_parquet_spark_job(
    caplog: LogCaptureFixture,
    spark_session: SparkSession,
    rand_dir_path: Path,
):
    # Given two csv files
    input_data1 = "id,val\n1,a"
    in_file_path1 = rand_dir_path / "data.csv"
    in_file_path1.write_text(input_data1)
    input_data2 = "id,name\n1,A"
    in_file_path2 = rand_dir_path / "lookup.csv"
    in_file_path2.write_text(input_data2)
    # And a job that joins the data and writes it to parquet
    out_dir_path = rand_dir_path / "out"
    job = TimedEtlJob(
        run_date=date(2022, 1, 1),
        steps=[
            CsvExtractor(
                input_path=in_file_path1,
                header="true",
            ).alias("data"),
            CsvExtractor(
                input_path=in_file_path2,
                header="true",
            ).alias("lookup"),
            AddTechnicalFieldsTransformer()
            .on_aliases("data", JOB_START_TIME)
            .alias("data_enriched"),
            JoinTransformer(on="id").on_aliases("data_enriched", "lookup"),
            ParquetLoader(
                mode="overwrite",
                output_path=out_dir_path,
            ),
        ],
    )
    # When the job is ran
    # Then there should not be an exception
    with caplog.at_level(logging.INFO):
        job.run()
    # And there should be a parquet filecreated
    out_files = list(out_dir_path.glob("*.parquet"))
    assert len(out_files) == 1
    # And it should contain the data
    actual_df = spark_session.read.parquet(str(out_files[0]))
    assert actual_df.columns == [
        "id",
        "val",
        "dl_ingestion_time",
        "dl_input_file_name",
        "name",
    ]
    assert actual_df.drop("dl_ingestion_time").collect() == [
        Row(
            id="1", val="a", dl_input_file_name="file://" + str(in_file_path1), name="A"
        )
    ]
    # And the logs should show the execution flow
    assert "Validating job" in caplog.text
    assert "Starting job" in caplog.text
    assert "CsvExtractor" in caplog.text
    # And the LogDurationSideEffect should log the duration
    assert "Job ran for " in caplog.text
    # Not checking all the others lines in the log...


def test_incorrect_key_fail(rand_path: Path):
    # Given a job that tries to use a non-existing key
    def generate_data() -> StringRecords:
        return ["hello", "world"]

    job = EtlJob(
        steps=[
            FunctionExtractor(generate_data).alias("foo"),
            TextFileLoader(rand_path).on_alias("bar"),
        ]
    )
    # When the job is validated
    # Then an exception should be thrown
    with pytest.raises(ValidationException) as e:
        job.validate()
    assert "Key bar was not found in EtlContext" in str(e)
