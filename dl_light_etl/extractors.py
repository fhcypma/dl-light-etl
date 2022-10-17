import logging
from abc import abstractmethod
from pathlib import Path
from types import FunctionType
from typing import Dict, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from dl_light_etl import DEFAULT_DATA_KEY
from dl_light_etl.types import AnyDataType, JobParameters, StringRecords
from dl_light_etl.utils import filesystem


class Extractors:
    """Basically a dict wrapper for multiple extractors

    Will run in sequence during job execution, placing the resulting data objects i nthe provided keys

    :Example:
    >>> Extractors() \
    >>> .add("world", world_extractor) \
    >>> .add("galaxy", galaxy_extractor)
    """

    def __init__(self) -> None:
        self.extractors: Dict[str, "AbstractExtractor"] = {}

    def add(self, *, key: str = None, extractor: "AbstractExtractor") -> "Extractors":
        """Adds an extractor to this Extractors

        :param str key: The key for the new extractor. Defaults to DEFAULT_DATA_KEY
        :param AbstracExtractor extractor: The new extractor
        :retuns: Itself, with the new extractor added
        """
        key = key if key else DEFAULT_DATA_KEY
        assert (
            key not in self.extractors.keys()
        ), f"Key [{key}] already defined in Extractors"
        self.extractors[key] = extractor
        return self

    def extract(
        self, parameters: JobParameters
    ) -> Tuple[JobParameters, Dict[str, AnyDataType]]:
        """Extract the data for all extractors

        :param JobParameters parameters: parameters in the job
        :returns: The updated job parameters, and the data retrieved by all extractor (mapped to the key), respectively
        :rtype: Tuple[JobParameters, Dict[str, AnyDataType]]
        """
        data_dict: Dict[str, AnyDataType] = {}
        for key, extractor in self.extractors.items():
            parameters, data = extractor.extract(parameters)
            data_dict[key] = data
        return parameters, data_dict


class AbstractExtractor:
    """Abstract class for generic extractor"""

    @abstractmethod
    def extract(self, parameters: JobParameters) -> Tuple[JobParameters, AnyDataType]:
        pass


class FunctionExtractor(AbstractExtractor):
    """Extractor that simply executes a given function

    Can be wrapped around, e.g., http API services

    :param FunctionType extraction_fct: The function to execute
    :param **fct_params: The parameters to pass to the function
    """

    def __init__(self, extraction_fct: FunctionType, **fct_params) -> None:
        self.extraction_fct = extraction_fct
        self.fct_params = fct_params

    def extract(self, parameters: JobParameters) -> Tuple[JobParameters, AnyDataType]:
        """Extract the data

        :param JobParameters parameters: The job parameters
        :returns: The updated job parameters and the extracted data, respecively
        :rtype: Tuple[JobParameters, AnyDataType]
        """
        logging.info(f"Starting {type(self)}")
        data = self.extraction_fct(**self.fct_params)
        return parameters, data


class TextFileExtractor(AbstractExtractor):
    """Extractor that reads in a file to lines of strings, without using spark

    :param JobParameters parameters: The job parameters
    :param
    """

    def __init__(self, input_path: Union[Path, str]) -> None:
        super().__init__()
        self.input_path = (
            input_path if type(input_path) == str else str(input_path.resolve())
        )

    def extract(self, parameters: JobParameters) -> Tuple[JobParameters, StringRecords]:
        """Extract the data

        :param JobParameters parameters: The job parameters
        :returns: The updated job parameters and the extracted data, respecively
        :rtype: Tuple[JobParameters, StringRecords]
        """
        logging.info(f"Starting {type(self)}")
        logging.info(f"Read file {self.input_path}")
        data = list(filesystem.read_text_file(path=self.input_path))
        return parameters, data


class DataFrameExtractor(AbstractExtractor):
    def __init__(
        self,
        spark: SparkSession,
        input_path: Union[Path, str],
        format: str,
        schema: StructType = None,
        **options,
    ) -> None:
        super().__init__()
        self.spark = spark
        self.input_path = (
            input_path if type(input_path) == str else str(input_path.resolve())
        )
        self.format = format
        self.schema = schema
        self.options = options

    def extract(self, parameters: JobParameters) -> Tuple[JobParameters, DataFrame]:
        logging.info(f"Starting {type(self)}")
        logging.info(f"Extracting text from {self.input_path}")
        df = self.spark.read.load(
            path=self.input_path.replace("s3:", "s3a:"),
            format=self.format,
            schema=self.schema,
            **self.options,
        )
        return (parameters, df)


class TextExtractor(DataFrameExtractor):
    def __init__(
        self,
        spark: SparkSession,
        input_path: Union[Path, str],
        schema: StructType = None,
        **options,
    ) -> None:
        options = options if options else {}
        super().__init__(spark, input_path, format="text", schema=schema, **options)


class CsvExtractor(DataFrameExtractor):
    def __init__(
        self,
        spark: SparkSession,
        input_path: Union[Path, str],
        schema: StructType = None,
        **options,
    ) -> None:
        options = options if options else {}
        super().__init__(spark, input_path, format="csv", schema=schema, **options)
