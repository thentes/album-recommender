# The recommendation engine for album similarity, based on tag weights (genres and descriptors)

import json

def __init__(self, data_path: str):
    
    with open(data_path, 'r') as f:
        data = json.load(f)

    feature_vocab = data['feature_vocab']
    self.albums = data['albums']

    # Quick lookup based on position
    self.position_to_album = {album['position']: album for album in self.albums}

    # Caches tag weights for each album
    self.album_tag_weights = {album['position']: self._build_album_tag_weights(album) for album in self.albums}

