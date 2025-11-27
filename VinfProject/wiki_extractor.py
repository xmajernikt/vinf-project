from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import os

def to_spark_path(path):
    abs_path = os.path.abspath(path)
    return "file:///" + abs_path.replace("\\", "/")

spark = SparkSession.builder \
    .appName("WikiCityExtractor") \
    .config("spark.local.dir", "D:/spark_tmp") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .getOrCreate()

xml_path = r"D:\Users\admin\Tomas\FIIT\ING\1.Semester\VINF\enwiki-latest-pages-articles.xml"
tsv_path = r"D:\Users\admin\Tomas\FIIT\ING\1.Semester\VINF\vinf_project\output\properties_single\properties.tsv"
parquet_output = "D:/Users/admin/Tomas/FIIT/ING/1.Semester/VINF/vinf_project/wiki_cities.parquet"

df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "page") \
    .load(to_spark_path(xml_path))

cities_df = spark.read.csv(to_spark_path(tsv_path), sep="\t", header=True)

cities = [row["city"] for row in cities_df.select("city").distinct().collect()]
cities = [c.lower().strip() for c in cities if c]
print(cities[0])
pattern = r"\b(" + "|".join([re.escape(c.lower()) for c in cities]) + r")\b"
df = df.filter(F.col("ns") == 0)


cities_df = cities_df.select(F.col("city").alias("city_name")).dropDuplicates(["city_name"])

# Rename title to avoid ambiguity
df = df.withColumnRenamed("title", "wiki_title")

# Join on lowercased comparison
city_df = df.join(
    cities_df,
    F.lower(F.col("wiki_title")) == F.lower(F.col("city_name")),
    "inner"
)

# Now select the desired columns safely
result = city_df.select(
    F.col("wiki_title").alias("title"),
    F.col("revision.text._VALUE").alias("content"),
    F.col("revision.timestamp").alias("last_updated")
)

result.write.mode("overwrite").parquet(parquet_output)

spark.stop()
print(f"Saved filtered Wikipedia pages to {parquet_output}")
