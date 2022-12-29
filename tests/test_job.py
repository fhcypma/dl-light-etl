import logging
from datetime import date
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pytest import LogCaptureFixture

from dl_light_etl.etl_constructs import EtlJob
from dl_light_etl.extractors import CsvExtractor
from dl_light_etl.loaders import ParquetLoader
from dl_light_etl.side_effects.timing import (
    JOB_START_TIME,
    JobStartTimeGetter,
    LogDurationSideEffect,
)
from dl_light_etl.transformers import AddTechnicalFieldsTransformer, JoinTransformer


def test_simple_csv_to_parquet_spark_job(
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
    job = EtlJob(
        run_date_or_time=date(2022, 1, 1),
        actions=[
            JobStartTimeGetter(),
            CsvExtractor(
                spark=spark_session,
                input_path=in_file_path1,
                header="true",
            ).with_output_key("data"),
            CsvExtractor(
                spark=spark_session,
                input_path=in_file_path2,
                header="true",
            ).with_output_key("lookup"),
            AddTechnicalFieldsTransformer()
            .with_input_keys("data", JOB_START_TIME)
            .with_output_key("data_enriched"),
            JoinTransformer(on="id").with_input_keys("data_enriched", "lookup"),
            ParquetLoader(
                mode="overwrite",
                output_path=out_dir_path,
            ),
            LogDurationSideEffect(),
        ],
    )
    # When the job is executed
    with caplog.at_level(logging.INFO):
        job.execute()
    # Then there should be a parquet filecreated
    out_files = list(out_dir_path.glob("*.parquet"))
    assert len(out_files) == 1
    print(out_files)
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
    assert "Starting job" in caplog.text
    assert (
        "Starting action <class 'dl_light_etl.side_effects.timing.JobStartTimeGetter'>"
        in caplog.text
    )
    assert (
        "Starting action <class 'dl_light_etl.extractors.CsvExtractor'>" in caplog.text
    )
    # Not checking all the others...
