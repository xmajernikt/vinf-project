from pyspark.sql import SparkSession
from extractor1 import ExtractorSpark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os, sys
import shutil, glob

import faulthandler; faulthandler.enable()
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

venv = sys.executable
os.environ["PYSPARK_PYTHON"] = venv
os.environ["PYSPARK_DRIVER_PYTHON"] = venv

spark = (
    SparkSession.builder
    .appName("RealEstateExtractor")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.executorEnv.PYTHONPATH", project_root)
    .config("spark.driver.extraClassPath", project_root)
    .config("spark.executor.extraClassPath", project_root)
    .getOrCreate()
)

schema = StructType([
    StructField("path", StringType(), True),
    StructField("title", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("offer_type", StringType(), True),
    StructField("address", StringType(), True),
    StructField("area", StringType(), True),
    StructField("rooms", StringType(), True),
    StructField("energy_class", StringType(), True),
    StructField("year_built", StringType(), True),
    StructField("condition", StringType(), True),
    StructField("city", StringType(), True),
    StructField("price", StringType(), True),
    StructField("price_per_m2", StringType(), True),
    StructField("rent_price", StringType(), True),
    StructField("monthly_costs", StringType(), True),
    StructField("deposit", StringType(), True),
    StructField("availability", StringType(), True),
    StructField("rental_term", StringType(), True),
    StructField("land_area", StringType(), True),
    StructField("source_country", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("error", StringType(), True),
])

def extract(file_tuple):
    path, html = file_tuple
    try:
        extractor = ExtractorSpark()
        result = extractor.extract_document((path, html))
        d = result
        d["path"] = path
        d["error"] = None
        return d
    except Exception as e:
        return {
            "path": path,
            "title": None,
            "property_type": None,
            "offer_type": None,
            "address": None,
            "area": None,
            "rooms": None,
            "energy_class": None,
            "year_built": None,
            "condition": None,
            "city": None,
            "price": None,
            "price_per_m2": None,
            "rent_price": None,
            "monthly_costs": None,
            "deposit": None,
            "availability": None,
            "rental_term": None,
            "land_area": None,
            "source_country": None,
            "source_url": None,
            "error": str(e)
        }
rdd = spark.sparkContext.wholeTextFiles(
    "file:///D:/Users/admin/Tomas/FIIT/ING/1.Semester/VINF/vinf_project/VinfProject/pages/Slovakia/"
)

rdd2 = spark.sparkContext.wholeTextFiles(
    "file:///D:/Users/admin/Tomas/FIIT/ING/1.Semester/VINF/vinf_project/VinfProject/pages/Austria/"
)

rdd = rdd.union(rdd2)
rdd = rdd.filter(lambda x: x[0].lower().endswith(".html"))

parsed = rdd.map(extract)

df = spark.createDataFrame(parsed, schema=schema)

output_dir = "output/properties_single"

(df.coalesce(1)
   .write
   .option("delimiter", "\t")
   .option("header", "true")
   .mode("overwrite")
   .csv(output_dir)
)

part_file = glob.glob(output_dir + "/part-*.csv")[0]
shutil.move(part_file, output_dir + "/properties.tsv")

