import datetime

from dl_light_etl.side_effects.timing import (
    JOB_START_TIME,
    JobStartTimeGetter,
    LogDurationSideEffect,
)


def test_log_time_action():
    # Given the time
    tic = datetime.datetime.now()
    # And an empty context
    context = {}
    # When the job start time is set
    action = JobStartTimeGetter()
    out_context = action.process(context)
    # Then the start time should be set
    toc = datetime.datetime.now()
    assert JOB_START_TIME in out_context
    assert tic <= out_context[JOB_START_TIME]
    assert out_context[JOB_START_TIME] <= toc


# def test_log_duration_side_effect(caplog):
#     # Given an etl context with a start time
#     start_time = datetime.datetime.now()
#     etl_context = EtlContext(variables={JOB_START_TIME: start_time})
#     # When the duration is logged
#     action = LogDurationSideEffect()
#     with caplog.at_level(logging.INFO):
#         output_parameters = action.execute(etl_context)
#     # Then the log should hold the duration
#     assert f"Job ran for " in caplog.text
#     assert f" seconds" in caplog.text
