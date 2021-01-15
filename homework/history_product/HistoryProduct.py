from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, asc


class HistoryProduct:
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """

    def __init__(self, primary_keys=None):
        self.primary_keys = primary_keys or ['id', 'name', 'score']

    def __join_safe_null(self, left_df, right_df, how, keys=None):
        keys = keys if keys else self.primary_keys
        safe_keys = [getattr(left_df, key).eqNullSafe(getattr(right_df, key)) for key in keys]
        return left_df.join(right_df, on=safe_keys, how=how)

    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        inserted = self.__join_safe_null(new_dataframe, old_dataframe, how='anti') \
            .withColumn('meta', lit('inserted')).withColumn('priority', lit(1))

        deleted = self.__join_safe_null(old_dataframe, new_dataframe, how='anti') \
            .withColumn('meta', lit('deleted')).withColumn('priority', lit(0))

        not_changed = self.__join_safe_null(new_dataframe, old_dataframe, how='semi', keys=['id', 'name', 'score']) \
            .withColumn('meta', lit('not_changed')).withColumn('priority', lit(1))

        pre_changed = self.__join_safe_null(new_dataframe, inserted, how='anti')
        changed = self.__join_safe_null(pre_changed, not_changed, how='anti') \
            .withColumn('meta', lit('changed')).withColumn('priority', lit(1))

        return inserted.union(deleted).union(not_changed).union(changed) \
            .sort(asc('id'), asc('priority')) \
            .drop('priority')
