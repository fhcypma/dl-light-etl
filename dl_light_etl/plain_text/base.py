import logging
from pathlib import Path
from typing import List, Union

from dl_light_etl.base import (
    DEFAULT_DATA_KEY,
    AbstractExtractor,
    AbstractLoader,
    AbstractSideEffect,
    SimpleDataValidationSideEffect,
)
from dl_light_etl.errors import DataException
from dl_light_etl.plain_text.filesystem import read_text_file, write_text_file
from dl_light_etl.types import StringRecords

##############
# Extractors #
##############


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
        data = list(read_text_file(path=self.input_path))
        return data


###########
# Loaders #
###########


class TextFileLoader(AbstractLoader):
    def __init__(self, output_path: Union[Path, str]) -> None:
        super().__init__()
        self.output_path = (
            output_path if type(output_path) == str else str(output_path.resolve())
        )

    def _execute(self, lines: StringRecords) -> None:
        logging.info(f"Load data to {self.output_path}")
        assert type(lines) == StringRecords or type(lines) == list

        write_text_file(path=self.output_path, content="\n".join(lines))


################
# Side Effects #
################


class LogDataSideEffect(AbstractSideEffect):
    """Prints the contents of the data
    Used for debugging purposes
    """

    def __init__(self, n: int = 20, truncate: int = 20) -> None:
        super().__init__(default_input_aliases=[DEFAULT_DATA_KEY])
        self.n = n
        self.truncate = truncate

    def _execute(self, data: StringRecords) -> None:
        logging.info(f"Showing data {self._input_aliases[0]}:")
        data: List[str] = data
        for line in data[: self.n]:
            logging.info(line[: self.truncate])


class RecordCountValidationSideEffect(SimpleDataValidationSideEffect):
    def __init__(self, expected_count: int) -> None:
        def validate_input_data(data_object: StringRecords) -> None:
            """Validate data on having exactly expected_count lines"""
            actual_count = len(data_object)
            if actual_count != expected_count:
                raise DataException(
                    f"DataFrame does not contain {expected_count} but {actual_count} records"
                )

        super().__init__(validation_fct=validate_input_data)
