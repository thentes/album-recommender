from api.recommender import AlbumRecommender
rec = AlbumRecommender('processed_data/albums_with_embeddings.json')
try:
    res = rec.recommend([10, 20], 3)
    for r in res: print(r)
except Exception as e:
    import traceback
    traceback.print_exc()
