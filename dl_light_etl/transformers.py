import datetime
import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Tuple, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from dl_light_etl import DEFAULT_DATA_KEY
from dl_light_etl.types import AnyDataType, DateOrDatetime, JobParameters


class AbstractTransformer:
    @abstractmethod
    def transform(
        self, parameters: JobParameters, data: Dict[str, AnyDataType]
    ) -> Tuple[JobParameters, AnyDataType]:
        pass


class IdentityTransformer(AbstractTransformer):
    """Transformer that does nothing"""

    def transform(
        self, parameters: JobParameters, data: Dict[str, AnyDataType]
    ) -> Tuple[JobParameters, AnyDataType]:
        return parameters, None


@dataclass
class TransformerAction:
    key: str = DEFAULT_DATA_KEY
    transformer: AbstractTransformer = IdentityTransformer()


class Transformers:
    """Transformer for chaining multiple Transformers together

    Will run in sequence during job execution, placing the resulting data objects in the provided keys

    :Example:
    >>> Transformers() \  # doctest: +SKIP
    >>> .add(TransformerAction("intermediate_df", DoSomethingTransformer())) \  # doctest: +SKIP
    >>> .add(TransformerAction("output_df", DoSomethingAdditionallyTransformer("intermediate_df")))  # doctest: +SKIP
    """

    def __init__(self) -> None:
        self.transformers: List[TransformerAction] = []

    def add(self, transformer: Union[AbstractTransformer, TransformerAction]):
        if issubclass(type(transformer), AbstractTransformer):
            self.transformers += [TransformerAction(transformer=transformer)]
        else:
            self.transformers += [transformer]
        return self

    def transform(
        self, parameters: JobParameters, data: Dict[str, AnyDataType]
    ) -> Tuple[JobParameters, Dict[str, AnyDataType]]:
        logging.info(f"Starting {type(self)}")

        data_out = data.copy()
        for transformer_action in self.transformers:
            parameters, data_new = transformer_action.transformer.transform(
                parameters, data_out
            )
            if data_new:
                data_out[transformer_action.key] = data_new
        return parameters, data_out


class SingleDataObjectTransformer(AbstractTransformer):
    def __init__(self, key: str = None) -> None:
        super().__init__()
        self.key = key if key else DEFAULT_DATA_KEY


class AddTechnicalFieldsTransformer(SingleDataObjectTransformer):
    """
    Adds following fields to (all) DataFrame(s):
    = dl_ingestion_time = datetime.now()
    - dl_input_file_name = pyspark.sql.functions.input_file_name()
    """

    def transform(
        self, parameters: JobParameters, data: Dict[str, AnyDataType]
    ) -> Tuple[JobParameters, AnyDataType]:
        logging.info(f"Starting {type(self)}")

        df = data[self.key]
        if type(df) != DataFrame:
            raise NotImplementedError(f"Cannot add field to data of type {type(data)}")

        now = datetime.datetime.now()
        df = df.withColumn("dl_ingestion_time", f.lit(now)).withColumn(
            "dl_input_file_name", f.input_file_name()
        )
        return parameters, df


class AddRunDateOrTimeTransformer(SingleDataObjectTransformer):
    """
    Adds dl_run_date or dl_run_datetime to all the DataFrame(s)
    """

    def __init__(self, run_date_or_time: DateOrDatetime, key: str = None) -> None:
        super().__init__(key)
        self.run_date_or_time = run_date_or_time
        self.col_name = (
            "dl_run_date"
            if type(self.run_date_or_time) == datetime.date
            else "dl_run_datetime"
        )

    def transform(
        self, parameters: JobParameters, data: AnyDataType
    ) -> Tuple[JobParameters, AnyDataType]:
        logging.info(f"Starting {type(self)}")

        df = data[self.key]
        if type(df) != DataFrame:
            raise NotImplementedError(f"Cannot add field to data of type {type(data)}")

        df = df.withColumn(self.col_name, f.lit(self.run_date_or_time))
        return parameters, df
