from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "rewrite dataset")
spark = SparkSession(sc)

# Read csv
rdd = sc.textFile("C:/Users/Alice/Desktop/spark_test/dataset/reddit_answers_long.csv")
df = spark.read.options(delimiter=';', inferSchema=True, header=True) \
               .csv("C:/Users/Alice/Desktop/spark_test/dataset/reddit_answers_long.csv")
df.show()
df.printSchema()

# Sequence format
rdd.map(lambda x: tuple(x.split(";", 1))).saveAsSequenceFile("C:/Users/Alice/Desktop/spark_test/dataset/sequence")

# Avro format
df.write.format("avro").save("C:/Users/Alice/Desktop/spark_test/dataset/data.avro")

# Parquet
# gzip
df.write.option("compression","gzip").format("parquet").mode('overwrite').save("C:/Users/Alice/Desktop/spark_test/dataset/parquet_gzip")
# Snappy
df.write.option("compression","snappy").format("parquet").mode('overwrite').save("C:/Users/Alice/Desktop/spark_test/dataset/parquet_snappy")
# LZO
df.write.option("compression","lzo").format("parquet").mode('overwrite').save("C:/Users/Alice/Desktop/spark_test/dataset/parquet_lzo")
# LZ4
df.write.option("compression","lz4").format("parquet").mode('overwrite').save("C:/Users/Alice/Desktop/spark_test/dataset/parquet_lz4")

# ORC
df.write.orc("C:/Users/Alice/Desktop/spark_test/dataset/orc")