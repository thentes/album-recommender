# The recommendation engine for album similarity, based on tag weights (genres and descriptors)

import json
from typing import Dict

PRIMARY_WEIGHT = 1.5
NON_PRIMARY_WEIGHT = 0.5

class AlbumRecommender:

    def __init__(self, data_path):
        """
        Intakes the str data_path
        """
        
        with open(data_path, 'r') as f:
            data = json.load(f)

        feature_vocab = data['feature_vocab']
        self.albums = data['albums']

        # Quick lookup based on position
        self.position_to_album = {album['position']: album for album in self.albums}

        # Caches tag weights for each album
        self.album_tag_weights = {album['position']: self._build_album_tag_weights(album) for album in self.albums}

        # Builds search enries that collapse variant releases
        self.search_entries = self._build_search_entries()

        print(f"Loaded {len(self.albums)} albums and {len(feature_vocab)} features")

    def _build_album_tag_weights(self, album):
        """
        The name is pretty self-explanatory... builds the tag weightings for a given album, returning a Dict of its tags and their corresponsding weights

        Intakes album, a Dict
        Returns tag_weights, a Dict[str, float]
        """

        tag_weights = {}

        # Intakes the str raw_tag and the float weight
        def add_tag(raw_tag, weight):
            
            # maps tag variants to a canonical form for matching
            def _canonicalize_tag(self, raw_tag):
                tag = self._normalize_text(raw_tag)
                if tag in self.CONTENT_VERSION_TAGS:
                    return "content_version"
                return tag
    
            tag = self._canonicalize_tag(raw_tag)
            if not tag:
                return  
            
            # In case a tag appears multiple times, if it's a primary genre at least once, that'll be what prevails over a non-primary tag
            existing = tag_weights.get(tag, 0.0)
            tag_weights[tag] = max(existing, weight)

        for genre in album.get('primary_genres', []) or []:
            add_tag(genre, self.PRIMARY_WEIGHT)

        for genre in album.get('secondary_genres', []) or []:
            add_tag(genre, self.NON_PRIMARY_WEIGHT)

        for descriptor in album.get('descriptors', []) or []:
            add_tag(descriptor, self.NON_PRIMARY_WEIGHT)
    
        return tag_weights

    def _build_search_entries(self):
        """

        Returns entries, a list of dicts
        """

        # A dict of a str and dict
        grouped = {}

        # For each album, adds them into grouped
        for album in self.albums:
            artist_name = album.get('artist_name', '')
            release_name = album.get('release_name', '')
            
            artist_norm = self._normalize_text(artist_name)
            title_norm = self._normalize_title(release_name)
            group_key = f"{artist_norm} | {title_norm}"

            # Adds key into grouped if it isn't already there
            if group_key not in grouped:
                    grouped[group_key] = {'artist_norm': artist_norm, 'title_norm': title_norm, 'albums': [], 'alias_titles': set()}

            grouped[group_key]['albums'].append(album)
            grouped[group_key]['alias_titles'].add(self._normalize_text(release_name))

        entries = []
        for group in grouped.values():
            rep = self._choose_group_rep(group['albums'])
            variant_count = len(group['albums'])

            # Blobs allow for dumb text matching when a user queries
            raw_search_blob = " ".join([group['artist_norm'], *group['alias_titles'], ])

            norm_search_blob = " ".join([group['artist_norm'], group['title_norm'], ])

            entries.append({
                'position': rep['position'],
                'artist_name': rep['artist_name'],
                'release_name': rep['release_name'],
                'source': rep.get('source', 'rym'),
                'display': f"{rep['artist_name']} - {rep['release_name']}",
                'variant_count': variant_count,
                'raw_search_blob': raw_search_blob,
                'norm_search_blob': norm_search_blob,
            })

        return entries

    def _normalize_text()

    def _normalize_title()

    def _choose_group_rep()


    """
    Work to still do:
    Finish up the unfinished functs above
    Do you actually need the blobs or alias_titles?
    Fix up add_tag
    Actually make the recommending funct
    """