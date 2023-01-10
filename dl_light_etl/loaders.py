import logging
from abc import abstractmethod
from pathlib import Path
from typing import List, Union

from pyspark.sql import DataFrame

from dl_light_etl.base import DEFAULT_DATA_KEY, EtlStep
from dl_light_etl.types import StringRecords
from dl_light_etl.utils import filesystem


class AbstractLoader(EtlStep):
    """Abstract class for saving a data object"""

    def __init__(self) -> None:
        super().__init__(default_input_aliases=[DEFAULT_DATA_KEY])

    @abstractmethod
    def _execute(self, **kwargs) -> None:
        pass


class TextFileLoader(AbstractLoader):
    def __init__(self, output_path: Union[Path, str]) -> None:
        super().__init__()
        self.output_path = (
            output_path if type(output_path) == str else str(output_path.resolve())
        )

    def _execute(self, lines: StringRecords) -> None:
        logging.info(f"Load data to {self.output_path}")
        assert type(lines) == StringRecords or type(lines) == list

        filesystem.write_text_file(path=self.output_path, content="\n".join(lines))


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
