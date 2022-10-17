import logging
from abc import abstractmethod
from pathlib import Path
from typing import Dict, List, Union

from dl_light_etl import DEFAULT_DATA_KEY
from dl_light_etl.types import AnyDataType, JobParameters
from dl_light_etl.utils import filesystem


class AbstractLoader:
    @abstractmethod
    def load(self, data, **kwargs) -> None:
        pass


class TextFileLoader(AbstractLoader):
    def __init__(self, output_path: Union[Path, str]) -> None:
        super().__init__()
        self.output_path = (
            output_path if type(output_path) == str else str(output_path.resolve())
        )

    def load(
        self, parameters: JobParameters, data: Dict[str, AnyDataType]
    ) -> JobParameters:
        logging.info(f"Starting {type(self)}")
        logging.info(f"Load data to {self.output_path}")
        lines = data[DEFAULT_DATA_KEY]

        filesystem.write_text_file(path=self.output_path, content="\n".join(lines))
        return parameters


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

    def load(
        self, parameters: JobParameters, data: Dict[str, AnyDataType]
    ) -> JobParameters:
        logging.info(f"Starting {type(self)}")
        logging.info(f"Loading data to {self.output_path}")
        df = data[DEFAULT_DATA_KEY]

        writer = df.write.mode(self.mode)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.parquet(self.output_path.replace("s3:", "s3a:"))
        return parameters
