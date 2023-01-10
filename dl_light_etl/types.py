import datetime
from typing import Any, Dict, List, Union

from pyspark.sql import DataFrame

# All variables in an ETL job
EtlContext = Dict[str, Any]

# Dummy variables holder; for validtion
DummyContext = Dict[str, type]

# Type for flat text file data
StringRecords = List[str]

# Supported data types
AnyDataType = Union[StringRecords, DataFrame]  # TODO add pandas dataframe?

# For holding run_date or run_datetime, depending on job frequency
DateOrDatetime = Union[datetime.date, datetime.datetime]
