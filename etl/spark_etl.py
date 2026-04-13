"""
PySpark ETL pipeline

processes the music dataset and creates weighted feature embeddings for each album
from genres, descriptors, and optionally Last.fm tags
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import ArrayType, StringType, MapType, FloatType
import json
import sys
import os

# adds current directory to path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from lastfm_enrichment import main as run_lastfm_enrichment
    LASTFM_AVAILABLE = True
except ImportError:
    LASTFM_AVAILABLE = False
    print("warning: Last.fm enrichment module not available")

try:
    from album_consolidation import (
        normalize_album_title,
    )
    CONSOLIDATION_AVAILABLE = True
except ImportError:
    CONSOLIDATION_AVAILABLE = False
    print("warning: album consolidation module not available")


def parse_csv_list(field_value):
    """
    parses a comma-separated list field from the RYM CSV
    handles quoted values ("a, b, c"), unquoted (a), and NA for missing
    """
    if not field_value or field_value == "NA":
        return []

    # removes outer quotes if present
    field_value = field_value.strip()
    if field_value.startswith('"') and field_value.endswith('"'):
        field_value = field_value[1:-1]

    # splits by comma and strips whitespace
    return [item.strip() for item in field_value.split(',')]


def create_weighted_features(primary_genres, secondary_genres, descriptors, lastfm_tags=None):
    """
    creates a weighted feature dict for an album

    weights indicate importance - higher is more important:
        primary genres: 4 (defines the album's main style)
        secondary genres: 2 (supporting genres)
        descriptors: 2 (characteristics like "atmospheric", "melancholic")
        Last.fm tags: 1 (community-added, less structured)

    returns a dict mapping feature names to weights
    """
    features = {}
    
    # adds primary genres with highest weight
    for genre in primary_genres:
        if genre:
            features[f"primary_{genre}"] = 4.0
    
    # adds secondary genres
    for genre in secondary_genres:
        if genre:
            features[f"secondary_{genre}"] = 2.0
    
    # adds descriptors
    for descriptor in descriptors:
        if descriptor:
            features[f"descriptor_{descriptor}"] = 2.0
    
    # adds Last.fm tags with lowest weight
    if lastfm_tags:
        for tag in lastfm_tags:
            if tag:
                # uses tag_ prefix to distinguish from RYM descriptors
                key = f"tag_{tag}"
                # adds to existing weight if tag already exists as a descriptor
                if key not in features:
                    features[key] = 1.0
                else:
                    # If tag already exists as RYM descriptor, increase weight
                    features[key] += 1.0
    
    return features


def consolidate_album_variants(albums_data):
    """
    consolidates album variants (Deluxe, Remastered, Remix, etc.) into single entries

    when the same album exists in multiple versions:
        1. groups variants by normalized album title
        2. merges features with combined weights
        3. combines descriptors
        4. returns canonical album entries
    """
    if not CONSOLIDATION_AVAILABLE:
        print("Consolidation module not available. Skipping variant consolidation.")
        return albums_data
    
    # groups albums by (artist, normalized title)
    consolidation_map = {}
    
    for album in albums_data:
        artist = album.get("artist_name", "")
        title = album.get("release_name", "")
        normalized_title = normalize_album_title(title)
        key = f"{artist.lower()}|||{normalized_title.lower()}"
        
        if key not in consolidation_map:
            consolidation_map[key] = []
        
        consolidation_map[key].append(album)
    
    # merges variants
    consolidated_albums = []
    
    for key, variants in consolidation_map.items():
        if len(variants) == 1:
            # no consolidation needed
            consolidated_albums.append(variants[0])
        else:
            # multiple variants - merge them
            primary_album = variants[0]  # uses first variant as base
            
            # merges features and descriptors from all variants
            merged_features = dict(primary_album["features"] or {})
            merged_descriptors = list(primary_album.get("descriptors", []) or [])

            for variant in variants[1:]:
                for feature_name, weight in (variant.get("features") or {}).items():
                    if feature_name in merged_features:
                        merged_features[feature_name] += weight
                    else:
                        merged_features[feature_name] = weight
                
                # combines unique descriptors
                for descriptor in variant.get("descriptors", []) or []:
                    if descriptor not in merged_descriptors:
                        merged_descriptors.append(descriptor)
            
            # creates consolidated entry
            consolidated_album = {
                "position": primary_album["position"],
                "artist_name": primary_album["artist_name"],
                "release_name": primary_album["release_name"],
                "release_date": primary_album.get("release_date"),
                "release_type": primary_album.get("release_type", "album"),
                "primary_genres": primary_album.get("primary_genres", []),
                "secondary_genres": primary_album.get("secondary_genres", []),
                "descriptors": merged_descriptors,
                "features": merged_features,
                "avg_rating": primary_album.get("avg_rating", 0.0),
                "rating_count": primary_album.get("rating_count", 0),
                "source": primary_album.get("source", "rym"),
                "variant_count": len(variants),  # tracks how many variants were merged
            }
            
            # copies Last.fm fields if present
            for field in ["lastfm_playcount", "lastfm_listeners", "lastfm_similar_albums"]:
                if field in primary_album:
                    consolidated_album[field] = primary_album[field]
            
            consolidated_albums.append(consolidated_album)
    
    # re-indexes positions
    for idx, album in enumerate(consolidated_albums):
        album["position"] = idx + 1
    
    return consolidated_albums


def add_lastfm_top_albums(albums_data):
    """
    appends Last.fm popular albums (if available) to RYM output

    loads from multiple Last.fm data sources, deduplicates against existing
    RYM albums, and maps tags to descriptors
    """
    # tries to load from multiple Last.fm data sources
    lastfm_files = [
        "processed_data/lastfm_artist_albums.json",      # New: Artist crawler data
        "processed_data/lastfm_tag_albums.json",         # Alternative: Tag harvester
    ]
    
    all_lastfm_records = []
    
    # loads all available files
    for lastfm_file in lastfm_files:
        if os.path.exists(lastfm_file):
            print(f"Loading Last.fm data from {lastfm_file}...")
            with open(lastfm_file, "r") as f:
                records = json.load(f)
                all_lastfm_records.extend(records)
                print(f"  Loaded {len(records)} albums")
    
    if not all_lastfm_records:
        print("No Last.fm album files found. Skipping Last.fm data merge.")
        return albums_data, 0

    # builds a set of existing keys to avoid duplicates
    # key format: "artist|||album" (normalized to lowercase)
    existing_keys = {
        normalize_album_key(album["artist_name"], album["release_name"])
        for album in albums_data
    }

    next_position = max(album["position"] for album in albums_data) + 1
    added_count = 0

    # processes each Last.fm record
    for record in all_lastfm_records:
        # gets album info
        artist_name = record.get("artist_name", "")
        release_name = record.get("release_name", "")
        album_key = normalize_album_key(artist_name, release_name)

        # skips empty records
        if not artist_name or not release_name:
            continue
        
        # skips duplicates already in the dataset
        if album_key in existing_keys:
            continue

        # extracts tag names from the Last.fm record
        tag_names = [(t.get("name") or "").strip().lower() for t in record.get("tags", []) if (t.get("name") or "").strip()]
        descriptors = tag_names[:30]  # uses first 30 tags as descriptors
        
        # creates weighted features (no genres from Last.fm, only tags)
        features = create_weighted_features([], [], descriptors, tag_names)

        # builds album record
        album_dict = {
            "position": next_position,
            "artist_name": artist_name,
            "release_name": release_name,
            "release_date": None,  # Last.fm doesn't always have release dates  
            "release_type": "album",
            "primary_genres": [],  # Last.fm uses tags, not structured genres  
            "secondary_genres": [],
            "descriptors": descriptors,
            "features": features,
            "avg_rating": 0.0,  # Last.fm doesn't have ratings
            "rating_count": 0,
            "source": "lastfm",  # marks where this data came from
            "lastfm_playcount": int(record.get("playcount", 0) or 0),
            "lastfm_listeners": int(record.get("listeners", 0) or 0),
            "lastfm_similar_albums": record.get("similar_albums", []),
        }

        # adds to dataset
        albums_data.append(album_dict)
        existing_keys.add(album_key)  # marks as processed
        next_position += 1
        added_count += 1

    return albums_data, added_count


def main():
    """
    PySpark ETL pipeline: reads RYM CSV + optional Last.fm data, builds weighted
    feature embeddings for all albums, and writes processed_data/albums_with_embeddings.json
    """
    
    print("starting PySpark ETL pipeline...")
    print("=" * 60)
    
    # step 1: fetch Last.fm enrichment data (skipped by default — takes 2+ hours)
    # to enable: pass --enable-lastfm argument
    lastfm_tags_dict = {}
    SKIP_LASTFM = "--enable-lastfm" not in sys.argv
    
    if SKIP_LASTFM:
        print("\nskipping Last.fm enrichment (use --enable-lastfm to enable)")
        print("note: using RYM data only — keeps processing fast (<5 minutes)")
    elif LASTFM_AVAILABLE:
        print("\nAttempting Last.fm enrichment...")
        print("\nwarning: this may take 2-4 hours for 5000 albums")
        try:
            enriched_tags, similar_albums = run_lastfm_enrichment()
            lastfm_tags_dict = enriched_tags
            if lastfm_tags_dict:
                print(f"Last.fm enrichment successful ({len(lastfm_tags_dict)} albums)")
            else:
                print("note: Last.fm API key not configured, using RYM data only")
        except KeyboardInterrupt:
            print("\nLast.fm enrichment interrupted, continuing without enrichment...")
            lastfm_tags_dict = {}
        except Exception as e:
            print(f"Last.fm enrichment failed: {e}")
            print("  continuing with RYM data only...")
            lastfm_tags_dict = {}
    else:
        print("note: Last.fm enrichment module not available, using RYM data only")
    
    # initializes Spark session
    spark = SparkSession.builder \
        .appName("AlbumRecommenderETL") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("\nReading CSV data...")
    
    # reads CSV file
    df = spark.read.csv(
        "rym_top5000.csv",
        header=True,
        inferSchema=True,
        escape='"'
    )
    
    print(f"Loaded {df.count()} albums")
    
    # registers udfs for parsing
    parse_csv_list_udf = udf(parse_csv_list, ArrayType(StringType()))
    
    # creates enhanced feature UDF that includes Last.fm tags
    def create_features_with_lastfm(primary_genres, secondary_genres, descriptors, position):
        lastfm_tags = lastfm_tags_dict.get(int(position), [])
        return create_weighted_features(primary_genres, secondary_genres, descriptors, lastfm_tags)
    
    create_features_udf = udf(create_features_with_lastfm, MapType(StringType(), FloatType()))
    
    print("parsing genres and descriptors...")

    # parses the fields
    df_parsed = df.withColumn("primary_genres_list", parse_csv_list_udf(col("primary_genres"))) \
        .withColumn("secondary_genres_list", parse_csv_list_udf(col("secondary_genres"))) \
        .withColumn("descriptors_list", parse_csv_list_udf(col("descriptors"))) \
        .withColumn("features", create_features_udf(
            col("primary_genres_list"),
            col("secondary_genres_list"),
            col("descriptors_list"),
            col("position")
        ))
    
    # collects all unique feature names to build vocabulary
    print("Building feature vocabulary...")
    
    all_primary = df_parsed.select(explode(col("primary_genres_list")).alias("genre")).distinct()
    all_secondary = df_parsed.select(explode(col("secondary_genres_list")).alias("genre")).distinct()
    all_descriptors = df_parsed.select(explode(col("descriptors_list")).alias("descriptor")).distinct()
    
    primary_genres_vocab = [f"primary_{row.genre}" for row in all_primary.collect()]
    secondary_genres_vocab = [f"secondary_{row.genre}" for row in all_secondary.collect()]
    descriptor_vocab = [f"descriptor_{row.descriptor}" for row in all_descriptors.collect()]
    
    # adds Last.fm tags to vocabulary
    lastfm_vocab = []
    if lastfm_tags_dict:
        all_lastfm_tags = set()
        for tags in lastfm_tags_dict.values():
            all_lastfm_tags.update(tags)
        lastfm_vocab = [f"tag_{tag}" for tag in all_lastfm_tags]
    
    # complete feature vocabulary
    feature_vocab = sorted(primary_genres_vocab + secondary_genres_vocab + descriptor_vocab + lastfm_vocab)
    
    print(f"Feature vocabulary size: {len(feature_vocab)} ({len(primary_genres_vocab)} genres + {len(descriptor_vocab)} descriptors + {len(lastfm_vocab)} Last.fm tags)")
    
    # selects final columns for output
    # these are all the columns needed for recommendations
    df_final = df_parsed.select(
        col("position"),
        col("artist_name"),
        col("release_name"),
        col("release_date"),
        col("release_type"),
        col("primary_genres_list"),
        col("secondary_genres_list"),
        col("descriptors_list"),
        col("features"),
        col("avg_rating"),
        col("rating_count")
    )
    
    # saves as JSON for the API to load
    print("Saving processed data...")
    
    # converts to pandas for easier JSON handling
    df_pandas = df_final.toPandas()
    
    # converts to list of dicts
    albums_data = []
    for _, row in df_pandas.iterrows():
        position = int(row["position"])
        
        # builds album dict with all metadata
        album_dict = {
            "position": position,
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
        
        # adds Last.fm enrichment if available
        if position in lastfm_tags_dict:
            album_dict["lastfm_tags"] = lastfm_tags_dict[position]
        
        albums_data.append(album_dict)
    
    # adds Last.fm chart albums if crawler output exists
    albums_data, added_lastfm_top_count = add_lastfm_top_albums(albums_data)

    # consolidates album variants
    print("\nConsolidating album variants...")
    albums_before = len(albums_data)
    albums_data = consolidate_album_variants(albums_data)
    albums_after = len(albums_data)
    if albums_before > albums_after:
        print(f"consolidated {albums_before - albums_after} duplicate variants")

    # rebuilds feature vocabulary from final merged dataset
    # includes all features used in the dataset
    final_feature_vocab = sorted({
        feature_name
        for album in albums_data
        for feature_name in album["features"].keys()
    })

    # saves processed data
    output_data = {
        "feature_vocabulary": final_feature_vocab,
        "albums": albums_data
    }
    
    with open("processed_data/albums_with_embeddings.json", "w") as f:
        json.dump(output_data, f)
    
    print(f"successfully processed {len(albums_data)} albums")
    if lastfm_tags_dict:
        albums_with_lastfm = sum(1 for album in albums_data if any('tag_' in f for f in album['features']))
        print(f"Enhanced {albums_with_lastfm} albums with Last.fm tags")
    if added_lastfm_top_count > 0:
        print(f"added {added_lastfm_top_count} non-duplicate albums from Last.fm chart dataset")
    print(f"output saved to: processed_data/albums_with_embeddings.json")
    
    # stops Spark session
    spark.stop()
    
    print("ETL pipeline complete")


if __name__ == "__main__":
    main()
