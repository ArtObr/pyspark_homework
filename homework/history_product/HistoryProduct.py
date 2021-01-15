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

    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        # None filter
        inserted = new_dataframe.join(old_dataframe, on=self.primary_keys, how='anti') \
            .withColumn('meta', lit('inserted')).withColumn('priority', lit(1))
        deleted = old_dataframe.join(new_dataframe, on=self.primary_keys, how='anti') \
            .withColumn('meta', lit('deleted')).withColumn('priority', lit(0))
        not_changed = new_dataframe.join(old_dataframe, on=['id', 'name', 'score']) \
            .withColumn('meta', lit('not_changed')).withColumn('priority', lit(1))
        changed = new_dataframe.join(inserted, on=self.primary_keys, how='anti') \
            .join(not_changed, on=self.primary_keys, how='anti') \
            .withColumn('meta', lit('changed')).withColumn('priority', lit(1))

        inserted.show()
        deleted.show()
        not_changed.show()
        changed.show()

        return inserted.union(deleted).union(not_changed).union(changed) \
            .sort(asc('id'), asc('priority')) \
            .drop('priority')
