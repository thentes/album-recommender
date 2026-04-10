# recommendation engine for album similarity based on tag weights

import json
import re

class AlbumRecommender:
    # recommender that ranks albums by weighted shared tag matches

    PRIMARY_WEIGHT = 1.5
    NON_PRIMARY_WEIGHT = 0.5
    CONFIDENCE_TARGET = 7

    # variant keywords that normalize album titles for autocomplete matching
    VARIANT_KEYWORDS_PATTERN = re.compile(
        r"\b(deluxe|remaster(?:ed)?|remix(?:es)?|edition|version|bonus|expanded|anniversary|original|explicit|clean)\b",
        re.IGNORECASE,
    )

    CONTENT_VERSION_TAGS = {
        "explicit",
        "explicit lyrics",
        "explicit content",
        "clean",
        "clean lyrics",
        "clean version",
    }
    
    def __init__(self, data_path):
        """
        loads processed album data and builds the feature index
        data_path is a path to the JSON file with processed album data
        """

        print(f"Loading album data from {data_path}...")
        
        with open(data_path, 'r') as f:
            data = json.load(f)
        
        feature_vocab = data['feature_vocab']
        self.albums = data['albums']

        # quick lookup by position
        self.position_to_album = {album['position']: album for album in self.albums}

        # caches tag weights per album for scoring
        self.album_tag_weights = {}
        for album in self.albums:
            position = album['position']
            weight = self._build_album_tag_weights(album)
            self.album_tag_weights[position] = weight

        # builds canonical search entries that collapse variant releases
        self.search_entries = self._build_search_entries()
        
        print(f"Loaded {len(self.albums)} albums with {len(feature_vocab)} features")

    # normalizes text so case doesn't matter for title matching
    def _normalize_text(self, value):
        return " ".join((value or "").strip().lower().split())

    # strips the most common variant suffixes for title matching
    def _normalize_title_for_matching(self, title):
        normalized = self._normalize_text(title)

        # removes bracketed variant suffixes
        normalized = re.sub(
            r"\s*[\[(].*?(deluxe|remaster(?:ed)?|remix(?:es)?|edition|version|bonus|expanded|anniversary|original|explicit|clean).*?[\])]$",
            "",
            normalized,
            flags=re.IGNORECASE,
        )

        # removes the non-bracketed variant suffixes
        normalized = re.sub(
            r"\s+(deluxe|remaster(?:ed)?|remix(?:es)?|edition|version|bonus|expanded|anniversary|original|explicit|clean)\.?$",
            "",
            normalized,
            flags=re.IGNORECASE,
        )

        return " ".join(normalized.split())

    # normalizes the query by removing the variant words
    def _normalize_query_for_matching(self, query):
        normalized = self._normalize_text(query)
        normalized = self.VARIANT_KEYWORDS_PATTERN.sub(" ", normalized)
        return " ".join(normalized.split())

    # maps tag variants to a canonical form for matching
    def _canonicalize_tag(self, raw_tag):
        tag = self._normalize_text(raw_tag)
        if tag in self.CONTENT_VERSION_TAGS:
            return "content_version"
        return tag

    def _build_album_tag_weights(self, album):
        """
        builds tag weights for an album

        in this scoring model, primary RYM genres are worth 1.5 pts,
        while all others are worth 0.5 pts
        """
        tag_weights = {}

        for genre in album.get('primary_genres', []) or []:
            tag = self._normalize_text(genre)
            if tag not in self.CONTENT_VERSION_TAGS:
                tag = self._canonicalize_tag(genre)
                if tag:
                    existing = tag_weights.get(tag, 0.0)
                    tag_weights[tag] = max(existing, self.PRIMARY_WEIGHT)

        for genre in album.get('secondary_genres', []) or []:
            tag = self._normalize_text(genre)
            if tag not in self.CONTENT_VERSION_TAGS:
                tag = self._canonicalize_tag(genre)
                if tag:
                    existing = tag_weights.get(tag, 0.0)
                    tag_weights[tag] = max(existing, self.NON_PRIMARY_WEIGHT)

        for descriptor in album.get('descriptors', []) or []:
            tag = self._normalize_text(descriptor)
            if tag not in self.CONTENT_VERSION_TAGS:
                tag = self._canonicalize_tag(descriptor)
                if tag:
                    existing = tag_weights.get(tag, 0.0)
                    tag_weights[tag] = max(existing, self.NON_PRIMARY_WEIGHT)

        return tag_weights

    # picks the best representative album for a variant group
    def _choose_group_representative(self, albums):
        best_album = albums[0]
        best_score = None

        for album in albums:
            raw_title = album.get('release_name', '')
            if not raw_title:
                raw_title = ''
                
            normalized = self._normalize_title_for_matching(raw_title)
            
            is_variant_title = 0
            if normalized != self._normalize_text(raw_title):
                is_variant_title = 1

            rating_count = album.get('rating_count', 0)
            if not rating_count:
                rating_count = 0
            else:
                rating_count = int(rating_count)
                
            position = album.get('position', 0)
            if not position:
                position = 0
            else:
                position = int(position)
                
            # lower score is better when sorting, so we use similar logic
            # sorting key was (is_variant_title, -rating_count, position)
            current_score = (is_variant_title, -rating_count, position)
            
            if best_score is None or current_score < best_score:
                best_score = current_score
                best_album = album

        return best_album

    # groups variant releases into canonical autocomplete entries
    def _build_search_entries(self):
        grouped = {}

        for album in self.albums:
            artist_name = album.get('artist_name', '')
            release_name = album.get('release_name', '')

            artist_norm = self._normalize_text(artist_name)
            title_norm = self._normalize_title_for_matching(release_name)
            group_key = f"{artist_norm}|||{title_norm}"

            if group_key not in grouped:
                grouped[group_key] = {
                    'artist_norm': artist_norm,
                    'title_norm': title_norm,
                    'albums': [],
                    'alias_titles': set(),
                }

            grouped[group_key]['albums'].append(album)
            grouped[group_key]['alias_titles'].add(self._normalize_text(release_name))

        entries = []
        for group in grouped.values():
            representative = self._choose_group_representative(group['albums'])
            variant_count = len(group['albums'])

            raw_search_blob = " ".join([
                group['artist_norm'],
                *group['alias_titles'],
            ])
            normalized_search_blob = " ".join([
                group['artist_norm'],
                group['title_norm'],
            ])

            rating_count_raw = representative.get('rating_count', 0)
            if not rating_count_raw:
                rating_count_val = 0
            else:
                rating_count_val = int(rating_count_raw)

            entries.append({
                'position': representative['position'],
                'artist_name': representative['artist_name'],
                'release_name': representative['release_name'],
                'display': f"{representative['artist_name']} - {representative['release_name']}",
                'variant_count': variant_count,
                'raw_search_blob': raw_search_blob,
                'normalized_search_blob': normalized_search_blob,
                'rating_count': rating_count_val,
            })

        return entries
    
    
    def get_album_by_position(self, position):
        """
        gets album by its position in the dataset
        
        position is the album's position (1-indexed)
            
        returns the album dictionary
        """
        return self.position_to_album.get(position)
    
    def find_album(self, artist, title):
        """
        finds an album by artist and title
        
        takes in...
            artist which is the artist name
            title which is the album's title
            
        returns...
            album dict if it's found
            None if it isn't
        """
        artist_lower = artist.lower()
        title_lower = title.lower()
        
        for album in self.albums:
            if (album['artist_name'].lower() == artist_lower and 
                album['release_name'].lower() == title_lower):
                return album
        return None
    
    def autocomplete(self, query, limit = 10):
        """
        gets album suggestions for autocomplete
        
        takes in...
            query which is the search query
            limit which is the max num of results
            
        returns a list of album dicts with matching names
        """
        query_lower = self._normalize_text(query)
        query_normalized = self._normalize_query_for_matching(query)
        matches = []

        for entry in self.search_entries:
            raw_match = query_lower in entry['raw_search_blob']
            normalized_match = query_normalized and query_normalized in entry['normalized_search_blob']

            if raw_match or normalized_match:
                display = entry['display']
                if entry['variant_count'] > 1:
                    display = f"{display} ({entry['variant_count']} versions)"

                matches.append({
                    'position': entry['position'],
                    'artist_name': entry['artist_name'],
                    'release_name': entry['release_name'],
                    'display': display,
                    'variant_count': entry['variant_count'],
                })

        # keeps a stable ordering by quality and relevance
        def sort_matches(x):
            variant_count = x.get('variant_count', 1)
            return (-variant_count, x['artist_name'], x['release_name'])
        
        matches.sort(key=sort_matches)
        return matches[:limit]

    def _calculate_raw_points(
        self,
        selected_tag_weights,
        candidate_tag_weights,
    ):
        # calculates raw matching points between a candidate album and selected albums
        raw_points = 0.0

        for selected_weights in selected_tag_weights:
            shared_tags = set(candidate_tag_weights.keys()) & set(selected_weights.keys())
            for tag in shared_tags:
                raw_points += selected_weights[tag] + candidate_tag_weights[tag]

        return raw_points

    # normalizes score by tag count and applies confidence scaling
    def _normalize_score(self, raw_points, candidate_tag_count):
        ratio_score = (raw_points / candidate_tag_count) * 100.0
        confidence_factor = min(1.0, candidate_tag_count / float(self.CONFIDENCE_TARGET))
        return round(ratio_score * confidence_factor)

    def _build_recommendation_result(
        self,
        album,
        normalized_score,
        raw_points,
        candidate_tag_count,
    ):
        # create response payload for a recommended album
        result = {
            'position': album['position'],
            'artist_name': album['artist_name'],
            'release_name': album['release_name'],
            'score': normalized_score,
            'raw_points': round(raw_points, 2),
            'tag_count': candidate_tag_count,
            'descriptors': album.get('descriptors', []),
            'avg_rating': album.get('avg_rating', 0.0),
            'rating_count': album.get('rating_count', 0),
            'primary_genres': album.get('primary_genres', []),
            'secondary_genres': album.get('secondary_genres', []),
        }

        return result
    
    def recommend(self, selected_positions, top_n = 50):
        """
        recs albums based on selected albums using weighted scoring of shared tags

        intakes...
            selected_positions, a list of the selected albums' positions
            top_n, number of recs to return

        returns list of album recs with scores, sorted by those final scores
            

        RYM primary genre tags are worth 1.5 pts - everything else is 0.5 pts
        this means each shared tag between selected and candidate albums are...
            primary + primary -> 3 pts
            primary + non-primary -> 2 pts
            non-primary + non-primary -> 1 pts

        these scores are then normalized to avoid bias toward albums with many tags...
            ratio_score = (raw_points / candidate_tag_count) * 100
            
            confidence_factor = min(1.0, candidate_tag_count / confidence_target)

            in this implementation, confidence_target is 7 for RYM and 8 for Last.fm

        then, final_score = round(ratio_score * confidence_factor)
        
        tiebreaker of same final_score:
        1. average rating (higher is better)
        2. number of ratings (higher is also better)
        """
        # gets selected albums
        selected_albums = []
        for pos in selected_positions:
            album = self.get_album_by_position(pos)
            if album is not None:
                selected_albums.append(album)
        
        if not selected_albums:
            return []
        
        selected_tag_weights = []
        for album in selected_albums:
            position = album['position']
            weights = self.album_tag_weights.get(position, {})
            selected_tag_weights.append(weights)
        
        # scores all albums except selected ones
        scores = []
        selected_positions_set = set(selected_positions)
        
        for album in self.albums:
            # skips the selected albums
            if album['position'] in selected_positions_set:
                continue

            candidate_tag_weights = self.album_tag_weights.get(album['position'], {})
            candidate_tag_count = len(candidate_tag_weights)
            if candidate_tag_count == 0:
                continue

            raw_points = self._calculate_raw_points(selected_tag_weights, candidate_tag_weights)

            if raw_points > 0:
                normalized_score = self._normalize_score(
                    raw_points,
                    candidate_tag_count
                )
                scores.append(
                    self._build_recommendation_result(
                        album,
                        normalized_score,
                        raw_points,
                        candidate_tag_count,
                    )
                )
        
        # sorts by score, then rating, then rating_count in descending order
        def sort_scores(x):
            score = x['score']
            avg_rating = x.get('avg_rating', 0.0)
            rating_count = x.get('rating_count', 0)
            return (score, avg_rating, rating_count)

        scores.sort(key=sort_scores, reverse=True)
        
        return scores[:top_n]
    
    def get_album_info(self, position):
        """
        gets detailed information about an album

        inputs position, which is the given album's position

        returns a dict of the album's info
        """
        album = self.get_album_by_position(position)
        if not album:
            return None

        info = {
            'position': album['position'],
            'artist_name': album['artist_name'],
            'release_name': album['release_name'],
            'release_date': album['release_date'],
            'release_type': album['release_type'],
            'descriptors': album.get('descriptors', []),
            'avg_rating': album.get('avg_rating', 0.0),
            'rating_count': album.get('rating_count', 0),
            'primary_genres': album.get('primary_genres', []),
            'secondary_genres': album.get('secondary_genres', []),
        }

        return info
