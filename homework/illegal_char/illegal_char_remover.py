from pyspark.sql.dataframe import DataFrame
from typing import List
from pyspark.sql.functions import col, udf


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        if not chars or replacement is None:
            raise ValueError
        self.chars = chars
        self.replacement = replacement


    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        remover_udf = udf(lambda data: data.translate({ord(i): self.replacement for i in self.chars}))
        return dataframe.withColumn(target_column, remover_udf(getattr(dataframe, source_column))).drop(source_column)
