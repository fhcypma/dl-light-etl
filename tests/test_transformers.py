from datetime import datetime

from dl_light_etl import DEFAULT_DATA_KEY
from dl_light_etl.transformers import (
    AddRunDateOrTimeTransformer,
    AddTechnicalFieldsTransformer,
    IdentityTransformer,
    TransformerAction,
    Transformers,
)
from dl_light_etl.types import StringRecords
from pyspark.sql import SparkSession


def test_identity_transformer():
    # Given some data
    input_data: StringRecords = ["some", "data"]
    # When the transformer is applied
    transformer = IdentityTransformer()
    output_parameters, output_data = transformer.transform(
        parameters={}, data={DEFAULT_DATA_KEY: input_data}
    )
    # Then no data should be returned
    assert output_data is None


def test_transformers(spark_session: SparkSession):
    # Given some data
    input_data = [("val1",), ("val2",)]
    input_df = spark_session.createDataFrame(input_data, ["value"])
    # And a run_date
    run_date = datetime.now().date()
    # And two transformers that do nothing
    transformers = (
        Transformers()
        .add(
            TransformerAction("intermediate_df", AddTechnicalFieldsTransformer("in_df"))
        )
        .add(
            TransformerAction(
                "out_df", AddRunDateOrTimeTransformer(run_date, "intermediate_df")
            )
        )
    )
    # When the transformers are both applied
    output_parameters, output_data = transformers.transform(
        parameters={}, data={"in_df": input_df}
    )
    # Then the data should be the same
    assert output_data["in_df"].collect() == input_data
    assert output_data["intermediate_df"].columns == [
        "value",
        "dl_ingestion_time",
        "dl_input_file_name",
    ]
    assert output_data["out_df"].columns == [
        "value",
        "dl_ingestion_time",
        "dl_input_file_name",
        "dl_run_date",
    ]
    assert output_data["out_df"].collect()[1][3] == run_date


def test_add_technical_fields_transformer(spark_session: SparkSession):
    # Given a dataframe
    input_data = [("val1",), ("val2",)]
    input_df = spark_session.createDataFrame(input_data, ["value"])
    # When the technical fields are added
    transformer = AddTechnicalFieldsTransformer()
    output_parameters, output_data = transformer.transform(
        parameters={}, data={DEFAULT_DATA_KEY: input_df}
    )
    # Then the data should contain the technical columns
    assert output_data.columns == ["value", "dl_ingestion_time", "dl_input_file_name"]


def test_add_run_date_or_time_transformer(spark_session: SparkSession):
    # Given a dataframe
    input_data = [("val1",), ("val2",)]
    input_df = spark_session.createDataFrame(input_data, ["value"])
    # When the technical fields are added
    transformer = AddRunDateOrTimeTransformer(run_date_or_time=datetime.now().date())
    output_parameters, output_data = transformer.transform(
        parameters={}, data={DEFAULT_DATA_KEY: input_df}
    )
    # Then the data should contain the technical columns
    assert output_data.columns == ["value", "dl_run_date"]
