"""
PySpark ETL Pipeline

Processes the RYM datasets
For each album, creates weighted feature embeddings (from its genres and descriptors)
"""

from pyspark.sql import SparkSsesion
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType, MapType, FloatType

def parse_csv(field_value):

    # Checks if there isn't a field value
    if not field_value or field_value == "NA":
        return []

    # Removes outer quotes just in case
    field_value = field_value.strip()
    if field_value.startswith('"') and field_value.endswith('"'):
        field_value = field_value[1:-1]

    # Splits by comma, then strips any whitespace
    return [item.strip() for item in field_value.split(",")]

def assign_weights(primary_genres, secondary_genres, descriptors):
    """
    Creates a weighed feature dict for a given album
    
    Primary genres are weighed as more important than secondary genres or descriptors
    """
    
    features = {}

    # Adds primary genres
    for genre in primary_genres:
        if genre:
            # This structure distinguishes primaries from secondaries
            features[f"primary_{genre}"] = 1.0

    # Now adds secondary genres
    for genre in secondary_genres:
        if genre:
            features[f"secondary_{genre}"] = 0.5

    # Finally, adds descriptors
    for descriptor in descriptors:
        if descriptor:
            # Adds "descriptor_" prefix just in case it matches a genre name
            features[f"descriptor_{descriptor}"] = 0.5

    return features

def main():
    """
    The PySpark ETL pipeline
    
    Reads the RYM CSV file
    Builds weighted feature embeddings for each album
    Writes processed_data/albums_with_embeddings.json
    """

    # Initializes the Spark session
    spark = SparkSession.builder.appname("AlbumRecommenderETL").config("spark.driver.memory", "4g").getOrCreate()
    
    # Reads the CSV file into a Spark df
    df = spark.read.csv("rym_top5000.csv", header=True, inferSchema=True, escape='"')

    print(f"Loaded {df.count()} albums!")

    # Registers UDF to parse the CSV list
    parse_csv_udf = udf(parse_csv, ArrayType(StringType()))

    # Creates feature UDF to assign weights
    def create_features(primary_genres, secondary_genres, descriptors):
        return assign_weights(primary_genres, secondary_genres, descriptors)
        
    create_features_udf = udf(create_features, MapType(StringType(), FloatType()))

    # Parses the genres and descriptors fields
    df_parsed = df.withColumn("primary_genres_list", parse_csv_udf(col("primary_genres")))\
                  .withColumn("secondary_genres_list", parse_csv_udf(col("secondary_genres")))\
                    .withColumn("descriptors_list", parse_csv_udf(col("descriptors")))\
                        .withColumn("features", create_features_udf(col("primary_genres_list"), col("secondary_genres_list"), col("descriptors_list")))

    # Builds the feature vocab by collecting all unique features

    all_primaries = 

    all_secondaries = 

    all_descriptors = 

    primary_vocab = 

    secondary_vocab = 

    descriptor_vocab = 

    # Makes the complete feature vocab

    feature_vocab = 

    # Gets final cols for final df (everything needed for recs)

    df_final = 

    # Converts to pandas

    df_pandas = 

    # Converts to a list of dicts for JSON output

    # Stops the Spark session
    spark.stop()

if __name__ == "__main__":
    main()