# The recommendation engine for album similarity, based on tag weights (genres and descriptors)

import json
from typing import Dict
import re


class AlbumRecommender:

    PRIMARY_WEIGHT = 1.5
    NON_PRIMARY_WEIGHT = 0.5
    MIN_TAGS_FOR_FULL_CONFIDENCE = 7


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


    def _normalize_text(self, value):
        return " ".join((value or "").strip().lower().split())


    def _normalize_title(self, title):
        normalized = self._normalize_text(title)

        # Removes variant suffixes w/ brackets
        normalized = re.sub(
            r"\s*[\[(].*?(deluxe|remaster(?:ed)?|remix(?:es)?|edition|version|bonus|expanded|anniversary|original|explicit|clean).*?[\])]$",
            "",
            normalized,
            flags=re.IGNORECASE,
        )

        # Removes variant suffixes w/o brackets
        normalized = re.sub(
            r"\s+(deluxe|remaster(?:ed)?|remix(?:es)?|edition|version|bonus|expanded|anniversary|original|explicit|clean)\.?$",
            "",
            normalized,
            flags=re.IGNORECASE,
        )

        return " ".join(normalized.split())


    def _choose_group_rep(self, albums):
        def score_key(album):
            source = album.get('source', 'rym')

            raw_title = album.get('release_name', '') or ''
            is_variant = 1 if self._normalize_title(raw_title) != self._normalize_text(raw_title) else 0
            
            rating_count = int(album.get('rating_count', 0) or 0)
            position = int(album.get('position', 0) or 0)
            return (is_variant, -rating_count, position)
        
        return sorted(albums, key=score_key)[0]
    

    def get_album_by_position(self, position):
        """
        Gets album by its position in the dataset
        
        Position is the album's position (1-indexed)
        
        Returns the album dict
        """

        return self.position_to_album.get(position)
        

    def calc_raw_points(self, selected_tag_weights, candidate_tag_weights):
        # Raw matching points between candidate album and selected album
        raw_points = 0.0

        for weights in selected_tag_weights:
            shared_tags = set(candidate_tag_weights.keys()) & set(selected_tag_weights.keys())
            for tag in shared_tags:
                raw_points += weights[tag] + candidate_tag_weights[tag]
        
        return raw_points
        

    def _normalize_score(self, raw_points, candidate_tag_count, source):
        ratio_score = (raw_points / candidate_tag_count) * 100.0

        confidence_target = self.MIN_TAGS_FOR_FULL_CONFIDENCE
        confidence_factor = min(1.0, candidate_tag_count / float(confidence_target))

        return round(ratio_score * confidence_factor)

        
    def _build_rec_result(self, album, norm_score, raw_points, candidate_tag_count):
        # Given the album rec, create response payload
        source = album.get('source', 'rym')
        result = {
            'position': album['position'],
            'artist_name': album['artist_name'],
            'release_name': album['release_name'],
            'score': norm_score,
            'raw_points': round(raw_points, 2),
            'tag_count': candidate_tag_count,
            'descriptors': album.get('descriptors', []),
            'source': source,
            'avg_rating': album.get('avg_rating', 0.0),
            'rating_count': album.get('rating_count', 0),
            'primary_genres': album.get('primary_genres', []),
            'secondary_genres': album.get('secondary_genres', [])
        }

        return result
    

    def recommend(self, selected_positions, top_n = 50):

        # Gets selected albums
        selected_albums = [self.get_album_by_position(pos) for pos in selected_positions]
        selected_albums = [a for a in selected_albums if a is not None]

        if not selected_albums:
            return []

        selected_tag_weights = [self.album_tag_weights.get(album['position'], {}) for album in selected_albums]

        # Scores all albums except the selected ones
        scores = []
        selected_pos_set = set(selected_positions)

        for album in self.albums:
            # Skips the selected albums
            if album['position'] in selected_pos_set:
                continue
        
            candidate_tag_weights = self.album_tag_weights.get(album['position'], {})
            candidate_tag_count = len(candidate_tag_weights)
            if candidate_tag_count == 0:
                continue
        
            raw_points = self._calc_raw_points(selected_tag_weights, candidate_tag_weights)
            
            if raw_points > 0:
                norm_score = self._normalize_score(raw_points, candidate_tag_count, album.get('source', 'rym'))
                scores.append(self._build_rec_result(album, norm_score, raw_points, candidate_tag_count))

        # Sorts by score, then rating, then rating count in desc order
        scores.sort(key=lambda x: (x['score'], x.get('avg_rating', 0.0), x.get('rating_count'), 0), reverse = True)

        return scores[:top_n]
