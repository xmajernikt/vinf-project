from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("../wiki_cities.parquet")

rows = df.take(5)

for i in [1, 4]:
    row = rows[i]
    print(f"\nRow {i+1}")
    print(f"Title: {row['title']}")
    print(f"Last updated: {row['last_updated']}")
    print(f"Content preview: {row['content'][:500]}")  # first 500 chars


df.printSchema()

pdf = df.limit(1000).toPandas()
print(pdf.head(100))

print(df[""])