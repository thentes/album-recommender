"""
PySpark ETL Pipeline

Processes the RYM datasets
For each album, creates weighted feature embeddings (from its genres and descriptors)
Saves a JSON with album data and the complete feature vocab
"""

import json

from pyspark.sql import SparkSsesion
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import ArrayType, StringType, MapType, FloatType

# Cleans up a comma-separated list
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

    # Registers user defined funct to parse the CSV list
    parse_csv_udf = udf(parse_csv, ArrayType(StringType()))

    # Creates feature UDF to assign weights
    def create_features(primary_genres, secondary_genres, descriptors):
        return assign_weights(primary_genres, secondary_genres, descriptors)
        
    create_features_udf = udf(create_features, MapType(StringType(), FloatType()))

    # Parses the genres and descriptors fields with parse_csv_udf, cleaning up each col of genres to make a parsed df
    df_parsed = df.withColumn("primary_genres_list", parse_csv_udf(col("primary_genres")))\
                  .withColumn("secondary_genres_list", parse_csv_udf(col("secondary_genres")))\
                    .withColumn("descriptors_list", parse_csv_udf(col("descriptors")))\
                        .withColumn("features", create_features_udf(col("primary_genres_list"), col("secondary_genres_list"), col("descriptors_list")))

    # Builds the feature vocab by collecting all unique features

    all_primaries = df_parsed.select(explode(col("primary_genres_list")).alias("genre")).distinct() # explode "explodes" the array of genres into distinct rows (one per genre)

    all_secondaries = df_parsed.select(explode(col("secondary_genres_list")).alias("genre")).disinct()

    all_descriptors = df_parsed.select(explode(col("descriptors_list")).alias("descriptor")).distinct()

    # Prefixes are once again included to distinguish types
    primary_vocab = [f"primary_{row.genre}" for row in all_primaries.collect() if row.genre]

    secondary_vocab = [f"secondary_{row.genre}" for row in all_secondaries.collect() if row.genre]

    descriptor_vocab = [f"descriptor_{row.descriptor}" for row in all_descriptors.collect() if row.descriptor]

    # Makes the complete feature vocab
    feature_vocab = primary_vocab + secondary_vocab + descriptor_vocab

    print(f"Feature vocab size: {len(feature_vocab)} with {len(primary_vocab)} primary genres, {len(secondary_vocab)} secondary genres, and {len(descriptor_vocab)} descriptors")

    # Gets final cols for final df (everything needed for recs)
    df_final = df_parsed.select(col("position"), col("artist_name"), col("release_name"), col("release_date"), col("release_type"), col("primary_genres_list"), col("secondary_genres_list"), col("descriptors_list"), col("features"), col("avg_rating"), col("rating_count"))

    # Converts final df to pandas
    df_pandas = df_final.toPandas()

    # Iterates over each pandas df row to convert to a list of dicts for JSON output
    albums_data = []
    for _, row in df_pandas.iterrows():

        album_dict = {
            "position": int(row["position"]),
            "artist_name": row["artist_name"],
            "release_name": row["release_name"],
            "release_date": row["release_date"],
            "release_type": row["release_type"],
            "primary_genres": row["primary_genres_list"],
            "secondary_genres": row["secondary_genres_list"],
            "descriptors": row["descriptors_list"],
            "features": row["features"],
            "avg_rating": float(row["avg_rating"]),
            "rating_count": int(row["rating_count"])
        }

        albums_data.append(album_dict)

    # Rebuilds complete feature vocab list from final, merged dataset
    final_feature_vocab_set = set()
    for album in albums_data:                 
        for feature_name in album["features"].keys():
            final_feature_vocab_set.add(feature_name)

    # Sorts the final feature vocab
    final_feature_vocab = sorted(list(final_feature_vocab_set))

    # Saves the processed data as JSON, including feature vocab
    output_data = {"feature_vocab": final_feature_vocab, "albums": albums_data}

    with open("processed_data/albums_with_embeddings.json", "w") as f:
        json.dump(output_data, f)

    print(f"Processed {len(albums_data)} albums and {len(final_feature_vocab)} unique features")

    # Stops the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
    