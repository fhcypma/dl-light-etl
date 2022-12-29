from datetime import datetime

from pyspark.sql import Row, SparkSession

from dl_light_etl.etl_constructs import RUN_DATE_KEY, RUN_TIME_KEY
from dl_light_etl.transformers import (
    AddRunDateOrTimeTransformer,
    AddTechnicalFieldsTransformer,
    JoinTransformer,
)


def test_add_technical_fields_transformer(spark_session: SparkSession):
    # Given a dataframe
    input_data = [("val1",), ("val2",)]
    input_df = spark_session.createDataFrame(input_data, ["value"])
    # And a time
    time = datetime.now()
    # When the technical fields are added
    transformer = AddTechnicalFieldsTransformer()
    output_data = transformer.execute(input_df, time)
    # Then the data should contain the technical columns
    assert output_data.columns == ["value", "dl_ingestion_time", "dl_input_file_name"]
    assert output_data.collect()[0][1] == time


def test_add_run_date_or_time_transformer(spark_session: SparkSession):
    # Given a dataframe
    input_data = [("val1",), ("val2",)]
    input_df = spark_session.createDataFrame(input_data, ["value"])
    # And a run time and date
    run_time = datetime.now()
    run_date = run_time.date()
    # When the run time is added
    transformer = AddRunDateOrTimeTransformer(run_date_or_time=run_time)
    output_data_with_time = transformer.execute(input_df)
    # Then the data should contain the time
    assert output_data_with_time.columns == ["value", RUN_TIME_KEY]
    assert output_data_with_time.collect()[0][1] == run_time

    # And when the run date is added
    transformer = AddRunDateOrTimeTransformer(run_date_or_time=run_date)
    output_data_with_date = transformer.execute(input_df)
    # Then the data should contain the date
    assert output_data_with_date.columns == ["value", RUN_DATE_KEY]
    assert output_data_with_date.collect()[0][1] == run_date


def test_join_transformer(spark_session: SparkSession):
    # Given two dataframes
    df1 = spark_session.createDataFrame([(1, "a")], ["id", "val"])
    df2 = spark_session.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
    # When the dataframes are joined
    transformer = JoinTransformer("id")
    output_data = transformer.execute(df1, df2)
    # Then the data should be joined
    assert output_data.collect()[0] == Row(id=1, val="a", name="A")
