# Album Recommender

A music recommendation project that suggests albums! It takes a user's selected albums and provides similar albums based on shared genres, descriptions, and tags.

# Stack
Data processing: pyspark
API: Flask
Conainerization: Docker and Docker Compose
CI/CD: GitHub actions
Recommendation engine: weighted tag scorer

## Architecture
csv/Last.fm -> pyspark -> processed json -> API scoring

ETL Service: runs pyspark, builds processed output
API Service: loads processed data, serves recommendations

## Starting w/ Docker
```
# Builds and runs the services
docker-compose up --build

# Opens the API
http://localhost:5000
```

## Scoring Logic
Primary RYM genres: 1.5 pts
All other tags: 0.5 pts

That means, when tags are shared between a given album and a candidate (result) album, this is how the candidate is scored:
Primary + Primary = 3 pts
Primary + Other = 2 pts
Other + Other = 1 pt
