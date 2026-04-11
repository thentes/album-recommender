"""
Last.fm enrichment script

fetches album data from the Last.fm API:
- tags (genre, mood, etc.)
- playcounts
- similar albums (for graph-based expansion)
...then caches the results

requires LASTFM_API_KEY and LASTFM_API_SECRET env variables
"""

import os
import json
import time
import pandas as pd
import pylast
from dotenv import load_dotenv

# loads env variables
load_dotenv()

# Last.fm api credentials

# key and secret are required for read-only operations
LASTFM_API_KEY = os.environ.get('LASTFM_API_KEY')
LASTFM_API_SECRET = os.environ.get('LASTFM_API_SECRET')

LASTFM_USERNAME = os.environ.get('LASTFM_USERNAME')
LASTFM_PASSWORD = os.environ.get('LASTFM_PASSWORD')
# username and password are only needed for authenticated requests

# cache files
LASTFM_CACHE_FILE = 'processed_data/lastfm_cache.json'
LASTFM_SIMILAR_ALBUMS_FILE = 'processed_data/lastfm_similar_albums.json'

NOISE_TAGS = {
    'seen live', 'albums i own', 'favorite', 'me',
    'own', 'owned', 'cover', 'remix', 'reissue', 'soundtrack',
    'needs work', 'untagged', 'to listen', 'loved'
}


# loads Last.fm cache from disk
def load_cache():
    if os.path.exists(LASTFM_CACHE_FILE):
        try:
            with open(LASTFM_CACHE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


# saves Last.fm cache to disk
def save_cache(cache):
    with open(LASTFM_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


# saves similar albums graph to disk
def save_similar_albums(similar_albums):
    with open(LASTFM_SIMILAR_ALBUMS_FILE, 'w') as f:
        json.dump(similar_albums, f, indent=2)


def fetch_album_enrichment(network, artist, album):
    """
    fetches album data from Last.fm

    takes in...
        network, which is the Last.fm network connection
        artist, the artist name
        album, the album name

    returns a dict with...
        tags, a list of all tags with weights {tag: weight}
        similar_albums, a list of similar albums {artist, title}
        playcount, a global play count on Last.fm
        or None if album not found
    """
    try:
        # tries to query Last.fm for the album
        lastfm_album = network.get_album(artist, album)
        
        try:
            all_tags = lastfm_album.get_tags() # tries to get all available tags
        except Exception:
            # if get_tags fails, tries get_top_tags
            try:
                all_tags = lastfm_album.get_top_tags()
            except Exception:
                all_tags = []
        
        # extracts tag names and weights
        tags_dict = {}
        tag_list = []
        for tag in all_tags:
            if tag and tag.item:
                tag_name = str(tag.item).lower().strip()
                
                if tag_name and len(tag_name) > 2 and tag_name not in NOISE_TAGS:
                    try:
                        # tries to get tag weight if available
                        weight = int(tag.weight) if hasattr(tag, 'weight') else 1
                    except Exception:
                        weight = 1
                    
                    tags_dict[tag_name] = weight
                    tag_list.append(tag_name)
        
        # tries to get similar albums
        similar_albums_list = []
        try:
            similar_albums = lastfm_album.get_similar()
            for sim_album in similar_albums[:12]:  # top 12 similar albums
                if sim_album:
                    sim_artist = sim_album.artist.name if hasattr(sim_album, 'artist') else None
                    sim_title = sim_album.title if hasattr(sim_album, 'title') else None
                    if sim_artist and sim_title:
                        similar_albums_list.append({
                            'artist': sim_artist,
                            'title': sim_title
                        })
        except Exception:
            pass
        
        # tries to get playcount
        playcount = 0
        try:
            playcount = int(lastfm_album.get_playcount())
        except Exception:
            pass
        
        return {
            'tags': tags_dict,        
            'tag_list': tag_list,       
            'similar_albums': similar_albums_list,  
            'playcount': playcount     
        }
    
    except pylast.WSError:
        # if album not found or API error
        return None
    except Exception as e:
        # rate limit or other error
        print(f"Error fetching data for {artist} - {album}: {e}")
        return None


def enrich_rym_dataset(network):
    """
    reads RYM dataset and enriches it with Last.fm tags and similar albums
    
    intakes network, the Last.fm network connection (is None if disabled)
        
    returns a tuple of...
        enriched_tags, a dict mapping album keys to tag lists
        similar_albums_graph, a dict mapping album keys to similar album lists
    """
    df = pd.read_csv('rym_top5000.csv')
    
    # loads existing cache
    cache = load_cache()
    
    enriched_tags = {}
    similar_albums_graph = {}
    total = len(df)
    
    print(f"Processing {total} albums from RYM dataset...")
    
    if network is None:
        print("Skipping Last.fm enrichment (API key not configured)")
        return enriched_tags, similar_albums_graph
    
    for idx, row in df.iterrows():
        artist = row['artist_name']
        album = row['release_name']
        position = row['position']
        
        # creates cache key
        cache_key = f"{artist} - {album}"
        
        # checks cache first
        if cache_key in cache:
            cached_data = cache[cache_key]
            if cached_data and 'tag_list' in cached_data:
                enriched_tags[position] = cached_data['tag_list']
                if 'similar_albums' in cached_data:
                    similar_albums_graph[position] = cached_data['similar_albums']
                if (idx + 1) % 100 == 0:
                    print(f"  [{idx + 1}/{total}] {artist} - {album} (cached)")
            elif (idx + 1) % 100 == 0:
                print(f"  [{idx + 1}/{total}] {artist} - {album} (not found in cache)")
        else:
            # fetches from Last.fm
            album_data = fetch_album_enrichment(network, artist, album)
            
            if album_data:
                tag_list = album_data['tag_list']
                similar_albums = album_data['similar_albums']
                
                enriched_tags[position] = tag_list
                if similar_albums:
                    similar_albums_graph[position] = similar_albums
                
                # caches the full data
                cache[cache_key] = album_data
                
                num_tags = len(tag_list)
                num_similar = len(similar_albums)
                print(f"  [{idx + 1}/{total}] {artist} - {album} → {num_tags} tags, {num_similar} similar")
            else:
                # in this case, marks as checked but not found
                cache[cache_key] = None
                if (idx + 1) % 100 == 0:
                    print(f"  [{idx + 1}/{total}] {artist} - {album} (not found)")
            
            # rate limiting: waits about 200ms between requests to respect API limits
            time.sleep(0.2)
    
    # saves caches
    save_cache(cache)
    save_similar_albums(similar_albums_graph)
    
    print(f"\nEnriched {len(enriched_tags)} albums with Last.fm tags")
    print(f"Found similar albums for {len(similar_albums_graph)} albums")
    
    return enriched_tags, similar_albums_graph


def get_lastfm_network(api_key, api_secret, username=None, password=None):
    """builds a Last.fm network client from env-backed credentials"""
    if not api_key:
        print("warning: LASTFM_API_KEY not found in environment")
        return None

    if not api_secret:
        print("warning: LASTFM_API_SECRET not found in environment")
        return None

    try:
        if username and password:
            network = pylast.LastFMNetwork(
                api_key=api_key,
                api_secret=api_secret,
                username=username,
                password=password,
            )
            print("connected to Last.fm api (authenticated mode)")
        else:
            network = pylast.LastFMNetwork(
                api_key=api_key,
                api_secret=api_secret,
            )
            print("connected to Last.fm api (read-only mode)")

        return network
    except Exception as error:
        print(f"failed to connect to Last.fm: {error}")
        return None


# the main entry point for Last.fm enrichment
def main():    
    # if it doesn't exist, creates processed_data directory
    os.makedirs('processed_data', exist_ok=True)
    
    # gets Last.fm network
    network = get_lastfm_network(
        LASTFM_API_KEY,
        LASTFM_API_SECRET,
        LASTFM_USERNAME,
        LASTFM_PASSWORD,
    )
    
    # enriches the dataset
    enriched_tags, similar_albums = enrich_rym_dataset(network)
        
    return enriched_tags, similar_albums

if __name__ == "__main__":
    main()
