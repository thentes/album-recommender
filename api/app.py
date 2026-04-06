"""
flask rest api for the album recommender

provides endpoints for autocomplete and album recommendations
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import socket
from recommender import AlbumRecommender

# gets project root directory (one level up from api/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# initializes flask app
app = Flask(__name__, static_folder='static')
CORS(app)

# initializes recommender
DATA_PATH = os.environ.get('DATA_PATH', os.path.join(PROJECT_ROOT, 'processed_data/albums_with_embeddings.json'))
recommender = None

# creates a json error response
def json_error(message: str, status_code: int):
    return jsonify({'error': message}), status_code

# parses an integer with a safe default for invalid input
def parse_int(value, default_value: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default_value

# initializes the recommender system
def init_recommender():
    global recommender
    if recommender is None:
        try:
            recommender = AlbumRecommender(DATA_PATH)
            print("Recommender initialized successfully")
        except Exception as e:
            print(f"Error initializing recommender: {e}")
            raise

# starts flask server and falls back to nearby ports if current one is busy
def run_server_with_port_fallback(host: str, initial_port: int, debug: bool):

    # checks if a host/port can be bound
    def is_port_available(port_to_check: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                test_socket.bind((host, port_to_check))
                return True
            except OSError:
                return False

    max_attempts = 5
    selected_port = None

    for offset in range(max_attempts):
        candidate_port = initial_port + offset
        if is_port_available(candidate_port):
            selected_port = candidate_port
            break

    if selected_port is None:
        raise RuntimeError(
            f"Could not start server. Ports {initial_port}-{initial_port + max_attempts - 1} are all in use."
        )

    if selected_port != initial_port:
        print(f"Port {initial_port} is busy. Falling back to {selected_port}.")

    print(f"Starting Flask server on {host}:{selected_port}")
    app.run(host=host, port=selected_port, debug=debug)

# serves the main page
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

# health check endpoint
@app.route('/health')
def health():
    return jsonify({'status': 'healthy'}), 200

@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    """
    autocomplete endpoint for the album search
    
    parameters for the query...
        "query" is the actual query string
        "limit" is the max number of results (default is 10)
    
    returns a JSON list of matching albums
    """
    try:
        if recommender is None:
            return json_error('Recommender is not initialized', 500)

        query = request.args.get('query', '')
        limit = parse_int(request.args.get('limit', 10), 10)
        limit = max(1, min(limit, 50))
        
        if not query or len(query) < 2:
            return jsonify([]), 200
        
        results = recommender.autocomplete(query, limit)
        return jsonify(results), 200
    
    except Exception as e:
        return json_error(str(e), 500)

@app.route('/recommend', methods=['POST'])
def recommend():
    """
    gets album recommendations based on user selected albums
    
    request body:
        {
            "selected_albums": [position1, position2, ...],
            "top_n": 50
        }
    
    returns a JSON list of recommended albums with scores
    """
    try:
        if recommender is None:
            return json_error('Recommender is not initialized', 500)

        data = request.get_json()
        
        if not data or 'selected_albums' not in data:
            return json_error('selected_albums is required', 400)
        
        selected_albums = data['selected_albums']
        top_n = parse_int(data.get('top_n', 50), 50)
        top_n = max(1, min(top_n, 5000))
        
        if not selected_albums:
            return json_error('At least one album must be selected', 400)
        
        recommendations = recommender.recommend(selected_albums, top_n)
        
        return jsonify({
            'recommendations': recommendations,
            'count': len(recommendations)
        }), 200
    
    except Exception as e:
        return json_error(str(e), 500)

@app.route('/album/<int:position>', methods=['GET'])
def get_album(position):
    """
    gets detailed information about a specific album
    
    "position" is a path param that identifies the album position
    
    returns a json of the album's info
    """
    try:
        if recommender is None:
            return json_error('Recommender is not initialized', 500)

        album = recommender.get_album_info(position)
        
        if not album:
            return json_error('Album not found', 404)
        
        return jsonify(album), 200
    
    except Exception as e:
        return json_error(str(e), 500)

if __name__ == '__main__':
    # initializes recommender before starting the server
    init_recommender()
    
    # gets configuration from environment variables
    host = os.environ.get('FLASK_HOST', '0.0.0.0')
    port = parse_int(os.environ.get('FLASK_PORT', 5000), 5000)
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

    run_server_with_port_fallback(host, port, debug)
