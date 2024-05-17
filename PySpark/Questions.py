from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("question1").getOrCreate()

data = [
    ("John", "python, sql"),
    ("Aravind", "Java,SQL,HTML"),
    ("Sridevi", "Python,sql,pyspark")
]

df = spark.createDataFrame(data, ["Name", "Skills"])

df = df.withColumn("Skill", explode(split(df["Skills"], ",")))
df.show()