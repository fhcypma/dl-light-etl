from pytest import LogCaptureFixture

from dl_light_etl.base import DEFAULT_DATA_KEY
from dl_light_etl.plain_text.base import *
from dl_light_etl.types import StringRecords


##############
# Extractors #
##############


def test_text_file_extractor(rand_path: Path):
    # Given a text file
    input_data = ["This", "is", "the", "data"]
    rand_path.write_text("\n".join(input_data))
    # When the file is read
    extractor = TextFileExtractor(input_path=rand_path).alias("out")
    output_context = extractor.process({})
    # Then the data should be read
    assert output_context["out"] == input_data


###########
# Loaders #
###########


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


################
# Side Effects #
################


def test_record_count_validation():
    # Given an etl context with a dataframe with 1 record
    context = {DEFAULT_DATA_KEY: ["val"]}
    # And an etl step to validate that the record count is 1
    step = RecordCountValidationSideEffect(expected_count=1)
    # When the step is processed
    # Then no exception is raised
    step.process(context)


def test_log_data_action_string(caplog: LogCaptureFixture):
    # Given an etl context with lines of data
    input_data: StringRecords = ["line1", "line2"]
    context = {"data": input_data}
    # And a side effect to log the data
    action = LogDataSideEffect().on_alias("data")
    # When side effect is processed
    with caplog.at_level(logging.INFO):
        action.process(context)
    # Then the data should be in the logs
    for line in input_data:
        assert line in caplog.text


def test_log_data_action_string_limited_output(caplog: LogCaptureFixture):
    # Given an etl context with lines of data
    input_data: StringRecords = ["line1", "line2"]
    context = {DEFAULT_DATA_KEY: input_data}
    # And a side effect to log one row of
    action = LogDataSideEffect(n=1)
    # When the data is logged
    with caplog.at_level(logging.INFO):
        action.process(context)
    # Then the data should be in the logs
    assert input_data[0] in caplog.text
    assert not input_data[1] in caplog.text
