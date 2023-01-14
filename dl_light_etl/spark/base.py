"""
EtlSteps that are a thin wrapper around the normal spark/ dataframe functions
There should be no functions here that are specific to the custom framework
"""

import logging
from abc import abstractmethod
from pathlib import Path
from typing import List, Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType

from dl_light_etl.base import (
    DEFAULT_DATA_KEY,
    AbstractExtractor,
    AbstractLoader,
    AbstractSideEffect,
    AbstractTransformer,
    SimpleDataValidationSideEffect,
)
from dl_light_etl.errors import DataException


class SparkDriven:
    """Fixing EtlSteps that require a SparkSession"""

    def requires_spark_session(self) -> bool:
        return True


##############
# Extractors #
##############


class AbstractDataFrameExtractor(SparkDriven, AbstractExtractor):
    """Abstract class for DataFrame extractor"""

    @abstractmethod
    def _execute(self, **kwargs) -> DataFrame:
        pass


class DataFrameExtractor(AbstractDataFrameExtractor):
    def __init__(
        self,
        input_path: Union[Path, str],
        format: str,
        schema: Optional[StructType] = None,
        **options,
    ) -> None:
        super().__init__()
        self.input_path = (
            input_path if type(input_path) == str else str(input_path.resolve())
        )
        self.format = format
        self.schema = schema
        self.options = options

    def _execute(self) -> DataFrame:
        logging.info(f"Extracting DataFrame from {self.input_path}")
        spark: SparkSession = SparkSession.getActiveSession()
        df = spark.read.load(
            path=self.input_path.replace("s3:", "s3a:"),
            format=self.format,
            schema=self.schema,
            **self.options,
        )
        return df


class TextExtractor(DataFrameExtractor):
    def __init__(
        self,
        input_path: Union[Path, str],
        schema: StructType = None,
        **options,
    ) -> None:
        options = options if options else {}
        super().__init__(input_path, format="text", schema=schema, **options)


class CsvExtractor(DataFrameExtractor):
    def __init__(
        self,
        input_path: Union[Path, str],
        schema: StructType = None,
        **options,
    ) -> None:
        options = options if options else {}
        super().__init__(input_path, format="csv", schema=schema, **options)


class ParquetExtractor(DataFrameExtractor):
    def __init__(
        self,
        input_path: Union[Path, str],
        schema: StructType = None,
        **options,
    ) -> None:
        options = options if options else {}
        super().__init__(input_path, format="parquet", schema=schema, **options)


################
# Transformers #
################


class AbstractDataFrameTransformer(SparkDriven, AbstractTransformer):
    """Abstract class for DataFrame transformer"""

    @abstractmethod
    def _execute(self, **kwargs) -> DataFrame:
        pass


class JoinTransformer(AbstractDataFrameTransformer):
    """Join two DataFrames"""

    def __init__(
        self, on: Union[str, List[str], Column, List[Column]], how: str = "inner"
    ) -> None:
        super().__init__()
        self.on = on
        self.how = how

    def _execute(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        return df1.alias(self._input_aliases[0]).join(
            df2.alias(self._input_aliases[1]), self.on, self.how
        )


class SelectTransformer(AbstractDataFrameTransformer):
    """Select (a) column(s)"""

    def __init__(self, *cols: Union[str, Column]) -> None:
        super().__init__()
        self.cols = cols

    def _execute(self, df: DataFrame) -> DataFrame:
        return df.select(*self.cols)


class FilterTransformer(AbstractDataFrameTransformer):
    """Static filter on (a) column(s)"""

    def __init__(self, condition: Union[Column, str]) -> None:
        super().__init__()
        self.condition = condition

    def _execute(self, df: DataFrame) -> DataFrame:
        return df.filter(self.condition)


###########
# Loaders #
###########


class AbstractDataFrameLoader(SparkDriven, AbstractLoader):
    """Abstract class for DataFrame loader"""

    pass


class ParquetLoader(AbstractLoader):
    def __init__(
        self,
        mode: str,
        output_path: Union[Path, str],
        partition_by: Union[str, List[str]] = None,
    ) -> None:
        super().__init__()
        self.mode = mode
        self.partition_by = (
            [partition_by] if type(partition_by) == str else partition_by
        )
        self.output_path = (
            output_path if type(output_path) == str else str(output_path.resolve())
        )

    def _execute(self, df: DataFrame) -> None:
        logging.info(f"Loading data to {self.output_path}")
        assert type(df) == DataFrame

        writer = df.write.mode(self.mode)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.parquet(self.output_path.replace("s3:", "s3a:"))


################
# Side Effects #
################


class RecordCountValidationSideEffect(SparkDriven, SimpleDataValidationSideEffect):
    """Validate data on having exact expected_count records"""

    def __init__(self, expected_count: int) -> None:
        def validate_input_data(df: DataFrame) -> None:
            actual_count = df.count()
            if actual_count != expected_count:
                raise DataException(
                    f"DataFrame does not contain {expected_count} but {actual_count} records"
                )

        super().__init__(validation_fct=validate_input_data)


class LogDataSideEffect(SparkDriven, AbstractSideEffect):
    """Prints the contents of the DataFrame

    Used for debugging purposes
    """

    def __init__(self, n: int = 20, truncate: int = 20, vertical: bool = False) -> None:
        super().__init__(default_input_aliases=[DEFAULT_DATA_KEY])
        self.n = n
        self.truncate = truncate
        self.vertical = vertical

    def _execute(self, df: DataFrame) -> None:
        logging.info(f"Showing DataFrame {self._input_aliases[0]}:")
        df: DataFrame = df
        logging.info("\n" + df._jdf.showString(self.n, self.truncate, self.vertical))
