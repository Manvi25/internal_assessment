from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, coalesce
spark = SparkSession.builder.appName("Question 2").getOrCreate()
schema = StructType([
    StructField("Name1", StringType(), True),
    StructField("Name2", StringType(), True),
    StructField("Name3", StringType(), True)])
data = [
    ("Aravind", None, None),
    ("John", None, None),
    (None, "Sridevi", None)
]
df = spark.createDataFrame(data, schema=schema)
df_names = df.select(coalesce(col("Name1"), col("Name2"), col("Name3")).alias("Names"))
df_names.show()