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
print(f"Found {len(cities)} unique cities")
print(f"First city: {cities[0]}")

df = df.filter(F.col("ns") == 0)

cities_df = cities_df.select(F.col("city").alias("city_name")).dropDuplicates(["city_name"])

df = df.withColumnRenamed("title", "wiki_title")

df = df.filter(
    ~F.lower(F.col("wiki_title")).contains("(disambiguation)") &
    ~F.lower(F.col("wiki_title")).contains("history of") &
    ~F.lower(F.col("wiki_title")).contains("list of") &
    ~F.col("wiki_title").contains("(")
)

print(f"Wikipedia pages after filtering: {df.count()}")

city_df = df.join(
    cities_df,
    F.lower(F.col("wiki_title")) == F.lower(F.col("city_name")),
    "inner"
)

print(f"Matched city pages: {city_df.count()}")

duplicate_check = city_df.groupBy("wiki_title").count().filter(F.col("count") > 1)
dup_count = duplicate_check.count()
if dup_count > 0:
    print(f"WARNING: Found {dup_count} duplicate titles after join:")
    duplicate_check.show(10, truncate=False)

result = city_df.select(
    F.col("wiki_title").alias("title"),
    F.col("revision.text._VALUE").alias("content"),
    F.col("revision.timestamp").alias("last_updated")
)

result = result.dropDuplicates(["title"])

print(f"Final result after deduplication: {result.count()}")

result_cities = result.select("title").distinct().count()
print(f"Unique cities in result: {result_cities}")

result.write.mode("overwrite").parquet(parquet_output)

spark.stop()
print(f"Saved {result.count()} filtered Wikipedia pages to {parquet_output}")