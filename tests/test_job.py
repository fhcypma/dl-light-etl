from pathlib import Path
from pyspark.sql import SparkSession, Row

from dl_light_etl.extractors import CsvExtractor, Extractors
from dl_light_etl.jobs import EtlJob
from dl_light_etl.loaders import ParquetLoader
from dl_light_etl.transformers import AddTechnicalFieldsTransformer, Transformers


def test_simple_csv_to_parquet_spark_job(spark_session: SparkSession, rand_path: Path):
    # Given a csv file
    input_data = "col1,col2\nval1,val2"
    in_file_path = rand_path / "input.csv"
    in_file_path.write_text(input_data)
    # And a job that converts it to parquet
    out_dir_path = rand_path / "out"
    job = EtlJob(
        extractors=Extractors().add(
            extractor=CsvExtractor(
                spark=spark_session,
                input_path=in_file_path,
                header="true",
            )
        ),
        transformers=Transformers().add(AddTechnicalFieldsTransformer()),
        loader=ParquetLoader(
            mode="overwrite",
            output_path=out_dir_path,
        ),
    )
    # When the job is executed
    job.execute()
    # Then there should be a parquet filecreated
    out_files = list(out_dir_path.glob("*.parquet"))
    assert len(out_files) == 1
    print(out_files)
    # And it should contain the data
    actual_df = spark_session.read.parquet(str(out_files[0]))
    assert actual_df.columns == [
        "col1",
        "col2",
        "dl_ingestion_time",
        "dl_input_file_name",
    ]
    assert actual_df.drop("dl_ingestion_time").collect() == [
        Row(col1="val1", col2="val2", dl_input_file_name="file://" + str(in_file_path))
    ]
