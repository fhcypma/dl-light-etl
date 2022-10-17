import datetime
from typing import Any, Dict, List, Union
from pyspark.sql import DataFrame


# Type for flat text file data
StringRecords = List[str]

# Supported data types
AnyDataType = Union[StringRecords, DataFrame]  # TODO add pandas dataframe?

# Parameters and data objects contained in a job
JobParameters = Dict[str, Any]

# For holding run_date or run_datetime, depending on job frequency
DateOrDatetime = Union[datetime.date, datetime.datetime]
