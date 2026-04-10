from api.recommender import AlbumRecommender
rec = AlbumRecommender('processed_data/albums_with_embeddings.json')
try:
    print(rec.autocomplete('vo', 10))
except Exception as e:
    import traceback
    traceback.print_exc()
