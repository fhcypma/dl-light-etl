import logging
from abc import abstractmethod
from pathlib import Path
from types import FunctionType
from typing import Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from dl_light_etl.base import DEFAULT_DATA_KEY, EtlStep
from dl_light_etl.types import AnyDataType, StringRecords
from dl_light_etl.utils import filesystem


class AbstractExtractor(EtlStep):
    """Abstract class for generic extractor"""

    def __init__(self) -> None:
        super().__init__(default_output_alias=DEFAULT_DATA_KEY)

    @abstractmethod
    def _execute(self, **kwargs) -> AnyDataType:
        pass


class FunctionExtractor(AbstractExtractor):
    """Extractor that simply executes a given function

    Can be wrapped around, e.g., http API services

    :param FunctionType extraction_fct: The function to execute
    :param **fct_params: The parameters to pass to the function
    """

    def __init__(self, extraction_fct: FunctionType, **fct_params) -> None:
        super().__init__()
        self.extraction_fct = extraction_fct
        self.fct_params = fct_params

    def _execute(self) -> AnyDataType:
        """Extract the data

        :returns: The extracted data
        :rtype: AnyDataType
        """
        data = self.extraction_fct(**self.fct_params)
        return data


class TextFileExtractor(AbstractExtractor):
    """Extractor that reads in a single file to lines of strings

    Does not use spark

    :param Union[Path, str] input_path: Path to the file
    """

    def __init__(self, input_path: Union[Path, str]) -> None:
        super().__init__()
        self.input_path = (
            input_path if type(input_path) == str else str(input_path.resolve())
        )

    def _execute(self) -> StringRecords:
        """Extract the data

        :returns: The extracted data
        :rtype: StringRecords
        """
        logging.info(f"Read file {self.input_path}")
        data = list(filesystem.read_text_file(path=self.input_path))
        return data


class DataFrameExtractor(AbstractExtractor):
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
