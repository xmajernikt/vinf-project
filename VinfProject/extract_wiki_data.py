from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
)

print(f"Using PySpark version: {spark.version}")


properties_path = "D:/Users/admin/Tomas/FIIT/ING/1.Semester/VINF/vinf_project/output/properties_single/properties.tsv"
wiki_parquet_path = "D:/Users/admin/Tomas/FIIT/ING/1.Semester/VINF/vinf_project/wiki_cities.parquet"
output_path = "D:/Users/admin/Tomas/FIIT/ING/1.Semester/VINF/vinf_project/properties_enriched_tsv"


props_df = spark.read.csv(properties_path, sep="\t", header=True)
wiki_df = spark.read.parquet(wiki_parquet_path)


def normalize(col):
    """
    Makes city names comparable.
    Removes commas, districts, hyphens, parentheses.
    Example: 'Pezinok, okres Pezinok' → 'pezinok'
    """
    return F.lower(
        F.trim(
            F.regexp_replace(col, r"[,\(\)\-–].*", "")
        )
    )

props_df = props_df.withColumn("city_norm", normalize(F.col("city")))
wiki_df = wiki_df.withColumn("title_norm", normalize(F.col("title")))


def extract_field(col, *field_names):

    result = F.lit(None)
    for field in field_names:
        pattern = rf"\|\s*{field}\s*=\s*([^\|\n]+)"
        extracted = F.regexp_extract(col, pattern, 1)
        result = F.when(
            (result.isNull() | (result == "")) & (extracted != ""),
            extracted
        ).otherwise(result)
    return result

def clean(col):

    col2 = F.regexp_replace(col, r"\{\{[^{}]*\}\}", "")
    col2 = F.regexp_replace(col2, r"\{\{[^{}]*\}\}", "")  
    col2 = F.regexp_replace(col2, r"\{\{.*?\}\}", "")     
    
    col2 = F.regexp_replace(col2, r"<[^>]*>", "")
    
    col2 = F.regexp_replace(col2, r"<ref[^>]*>.*?</ref>", "")
    col2 = F.regexp_replace(col2, r"<ref[^>]*/>", "")
    
    col2 = F.regexp_replace(col2, r"\[\[([^\]|]+)\|([^\]]+)\]\]", "$2")
    col2 = F.regexp_replace(col2, r"\[\[([^\]]+)\]\]", "$1")
    
    col2 = F.regexp_replace(col2, r"'{2,}", "")
    
    col2 = F.regexp_replace(col2, r"&[a-zA-Z]+;", " ")
    col2 = F.regexp_replace(col2, r"&#[0-9]+;", " ")
    
    col2 = F.regexp_replace(col2, r"[{}]", "")
    
    col2 = F.regexp_replace(col2, r"[\[\]]", "")
    
    col2 = F.regexp_replace(col2, r"\s+", " ")
    
    return F.trim(col2)


content_col = F.col("content")

wiki_df = (
    wiki_df
        .withColumn("population", 
            clean(extract_field(content_col, "population_total", "population", "pop")))
        .withColumn("area_km2", 
            clean(extract_field(content_col, "area_total_km2", "area_km2", "area")))
        .withColumn("elevation_m", 
            clean(extract_field(content_col, "elevation_m", "elevation")))
        
        .withColumn("country", 
            clean(extract_field(content_col, "country", "subdivision_name")))
        .withColumn("timezone", 
            clean(extract_field(content_col, "timezone", "timezone1", "utc_offset")))
        .withColumn("postal_code", 
            clean(extract_field(content_col, "postal_code", "postcode")))
        .withColumn("website", 
            clean(extract_field(content_col, "website", "url")))
        .withColumn("mayor", 
            clean(extract_field(content_col, "leader_name", "mayor", "leader_name1")))
        .withColumn("founded", 
            clean(extract_field(content_col, "established_date", "founded")))
        
        .withColumn("climate", 
            clean(extract_field(content_col, "climate", "climate_type", "Köppen")))
        .withColumn("coordinates", 
            clean(extract_field(content_col, "coordinates", "coord")))
        
        .withColumn("rivers_infobox", 
            clean(extract_field(content_col, "river", "rivers")))
        .withColumn("rivers_text1", 
            F.regexp_extract(content_col, r"(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\s+River", 1))
        .withColumn("rivers_text2", 
            F.regexp_extract(content_col, r"River\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})", 1))
        .withColumn("rivers", 
            F.concat_ws(", ", 
                F.col("rivers_infobox"),
                F.when(F.col("rivers_text1") != "", F.col("rivers_text1")),
                F.when(F.col("rivers_text2") != "", F.col("rivers_text2"))
            ))
        
        .withColumn("mountains1", 
            F.regexp_extract(content_col, r"(?:Mount|Mt\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", 1))
        .withColumn("mountains2", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Mountain", 1))
        .withColumn("mountains", 
            F.concat_ws(", ",
                F.when(F.col("mountains1") != "", F.col("mountains1")),
                F.when(F.col("mountains2") != "", F.col("mountains2"))
            ))
        
        .withColumn("historic1", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Castle|Cathedral|Palace)", 1))
        .withColumn("historic2", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Church|Fort|Fortress)", 1))
        .withColumn("historic3", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Monument|Memorial|Museum)", 1))
        .withColumn("historic4", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Old Town|Square)", 1))
        .withColumn("historic_sites", 
            F.concat_ws(", ",
                F.when(F.col("historic1") != "", F.col("historic1")),
                F.when(F.col("historic2") != "", F.col("historic2")),
                F.when(F.col("historic3") != "", F.col("historic3")),
                F.when(F.col("historic4") != "", F.col("historic4"))
            ))
        
        .withColumn("uni1", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+University", 1))
        .withColumn("uni2", 
            F.regexp_extract(content_col, r"University\s+of\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", 1))
        .withColumn("universities", 
            F.concat_ws(", ",
                F.when(F.col("uni1") != "", F.col("uni1")),
                F.when(F.col("uni2") != "", F.concat(F.lit("University of "), F.col("uni2")))
            ))
        
        .withColumn("airport1", 
            F.regexp_extract(content_col, r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:International\s+)?Airport", 1))
        .withColumn("airports", 
            F.when(F.col("airport1") != "", F.col("airport1")).otherwise(""))
        
        .withColumn("nickname_raw", 
            extract_field(content_col, "nickname", "nicknames"))
        .withColumn("nicknames", 
            F.regexp_replace(clean(F.col("nickname_raw")), r"[,;]", ", "))
        
        .withColumn("language_raw", 
            extract_field(content_col, "official_language", "languages", "language"))
        .withColumn("languages", 
            F.regexp_replace(clean(F.col("language_raw")), r"[,;]", ", "))
        
        .withColumn("twin_raw", 
            extract_field(content_col, "twin_cities", "sister_cities", "twin1"))
        .withColumn("twin_cities", 
            F.regexp_replace(clean(F.col("twin_raw")), r"[,;]", ", "))
        
        .drop("content", 
              "rivers_infobox", "rivers_text1", "rivers_text2",
              "mountains1", "mountains2",
              "historic1", "historic2", "historic3", "historic4",
              "uni1", "uni2", "airport1",
              "nickname_raw", "language_raw", "twin_raw")
)


def final_clean(col_name):
    """Apply final cleanup to remove any remaining wiki markup"""
    col = F.col(col_name)
    col = F.regexp_replace(col, r"\{\{[^}]*\}\}", "")
    col = F.regexp_replace(col, r"[\[\]{}]", "")
    col = F.regexp_replace(col, r"<[^>]*>", "")
    col = F.regexp_replace(col, r"\s+", " ")
    col = F.regexp_replace(col, r"^[,\s]+|[,\s]+$", "")
    return F.trim(col)

wiki_df = wiki_df.select(
    "title_norm",
    final_clean("population").alias("population"),
    final_clean("area_km2").alias("area_km2"),
    final_clean("elevation_m").alias("elevation_m"),
    final_clean("country").alias("country"),
    final_clean("coordinates").alias("coordinates"),
    final_clean("timezone").alias("timezone"),
    final_clean("postal_code").alias("postal_code"),
    final_clean("website").alias("website"),
    final_clean("mayor").alias("mayor"),
    final_clean("founded").alias("founded"),
    final_clean("climate").alias("climate"),
    final_clean("rivers").alias("rivers"),
    final_clean("mountains").alias("mountains"),
    final_clean("historic_sites").alias("historic_sites"),
    final_clean("universities").alias("universities"),
    final_clean("nicknames").alias("nicknames"),
    final_clean("airports").alias("airports"),
    final_clean("languages").alias("languages"),
    final_clean("twin_cities").alias("twin_cities")
)


joined_df = (
    props_df.join(
        wiki_df,
        on=(props_df.city_norm == wiki_df.title_norm),
        how="left"
    )
    .drop("city_norm", "title_norm")   
)

(
    joined_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("sep", "\t")
        .option("header", True)
        .csv(output_path)
)


spark.stop()