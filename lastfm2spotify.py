import requests
import csv
import os
import time
import json
import pandas as pd
from base64 import b64encode
from collections import deque
import random

# Configuration for Last.fm API
LASTFM_API_KEY = 'your_api_key'
ARTIST_FILE = 'artist.csv'
TRACKS_PER_PAGE = 50  # Number of tracks to retrieve per page

# Spotify credentials
SPOTIFY_CLIENT_ID = 'your_spotify_client_id'
SPOTIFY_CLIENT_SECRET = 'your_spotify_client_secret'

def fetch_lastfm_top_tracks(tag, page):
    """Fetch top tracks from Last.fm with retry logic."""
    if tag.lower() == "none":  # Special case for no tag
        url = f"http://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key={LASTFM_API_KEY}&format=json&page={page}"
    else:
        url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettoptracks&tag={tag}&api_key={LASTFM_API_KEY}&format=json&page={page}"
    
    retry_attempts = 5
    for attempt in range(retry_attempts):
        try:
            response = requests.get(url)
            if response.status_code == 429:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Rate limit reached for Last.fm. Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            
            # Ensure proper parsing
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"Invalid JSON response from Last.fm API: {response.text}")
                raise
            
            if tag.lower() == "none":
                return data.get('tracks', {}).get('track', [])
            else:
                return data.get('tracks', {}).get('track', [])
        except Exception as e:
            print(f"Error in fetch_lastfm_top_tracks (attempt {attempt + 1}/{retry_attempts}): {str(e)}")
            if attempt == retry_attempts - 1:
                raise



def process_tags(tags):
    """Process multiple tags and store results in artist CSV file."""
    random.shuffle(tags)  # Randomize the order of tags
    tag_queues = {tag: deque() for tag in tags}  # Dictionary to store queues for each tag
    tag_pages = {tag: 1 for tag in tags}  # Dictionary to track the current page for each tag
    seen_tracks = set()  # Set to keep track of seen tracks

    # Check for existing tag files and load data if available
    tag_files = {}
    for tag in tags:
        tag_file = f"{tag.strip()}.csv"
        if os.path.exists(tag_file):
            with open(tag_file, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    track_key = f"{row['Artist']} - {row['Title']}"
                    if track_key not in seen_tracks:
                        tag_queues[tag].append([row['Artist'], row['Title']])
                        seen_tracks.add(track_key)
            print(f"Loaded existing data from '{tag_file}'.")
        else:
            tag_files[tag] = open(tag_file, "a", newline="", encoding="utf-8")
            if os.path.getsize(tag_file) == 0:  # Write headers if file is empty
                csv.writer(tag_files[tag]).writerow(["Artist", "Title"])

    # Open artist.csv for writing
    with open(ARTIST_FILE, "a", newline="", encoding="utf-8") as artist_csvfile:
        artist_writer = csv.writer(artist_csvfile)
        
        if os.path.getsize(ARTIST_FILE) == 0:  # Only write headers if file is empty
            artist_writer.writerow(["Artist", "Title"])  # Header for the CSV file

        tag_index = 0
        while True:
            tag = tags[tag_index].strip()
            page = tag_pages.get(tag)

            if page is not None:  # Check if the page number is valid
                tracks_page = fetch_lastfm_top_tracks(tag, page)
                if tracks_page:
                    # Sort tracks based on the custom key
                    sorted_tracks = sorted(tracks_page, key=track_sort_key)

                    tag_pages[tag] += 1  # Move to the next page for the current tag

                    for track in sorted_tracks:
                        track_key = f"{track['artist']['name']} - {track['name']}"
                        if track_key not in seen_tracks:
                            track_data = [track['artist']['name'], track['name']]
                            tag_queues[tag].append(track_data)
                            seen_tracks.add(track_key)

                    print(f"Retrieved {len(tracks_page)} tracks for tag '{tag}' (page {page}).")
                else:
                    print(f"No more tracks available for tag '{tag}'.")
                    tag_pages[tag] = None  # Mark this tag as exhausted


            # Interleave tracks from all tags
            any_tags_active = False
            for i in range(len(tags)):
                current_tag = tags[(tag_index + i) % len(tags)]
                if tag_queues[current_tag]:
                    track = tag_queues[current_tag].popleft()
                    artist_writer.writerow(track)
                    csv.writer(tag_files.get(current_tag, None)).writerow(track)
                    any_tags_active = True

            if not any_tags_active:
                print("No more tracks available from any tag. Ending retrieval.")
                break

            # Move to the next tag after processing all active tags
            tag_index = (tag_index + 1) % len(tags)

        # Close tag files
        for file in tag_files.values():
            file.close()


def get_spotify_access_token():
    """Request a new access token from Spotify."""
    auth_headers = {"Authorization": f"Basic {b64encode(f'{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}'.encode('utf-8')).decode('utf-8')}"}
    auth_data = {"grant_type": "client_credentials"}
    auth_response = requests.post("https://accounts.spotify.com/api/token", headers=auth_headers, data=auth_data)
    
    if auth_response.status_code != 200:
        print("Authentication failed.")
        exit()
    
    access_token = json.loads(auth_response.text)["access_token"]
    return access_token

def preprocess_track_title(title):
    """Remove additional details from the track title."""
    # This simple split removes details after a hyphen, if present
    parts = title.split(' - ', 1)
    return parts[0].strip() if parts else title.strip()

def refresh_token_and_retry(request_func, *args, **kwargs):
    """Refresh the access token and retry the request."""
    global access_token
    access_token = get_spotify_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    return request_func(*args, **kwargs, headers=headers)

def update_processed_status(filename, track_title, track_id=None):
    """Updates the PROCESSED status in artist.csv for a given track if a valid track ID is found."""
    df = pd.read_csv(filename, low_memory=False, encoding="utf-8")
    if track_id:  # Only update if a valid track ID is found
        df.loc[df['Title'] == track_title, 'PROCESSED'] = 'Yes'
    df.to_csv(filename, index=False, encoding="utf-8")


def remove_duplicate_tracks(filename, track_id):
    """Checks if a track ID already exists in the results CSV."""
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row and row[1] == track_id:
                    return True
    return False

def get_retrieved_tracks_count(filename):
    """Counts the number of tracks already retrieved from results.csv."""
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            return sum(1 for row in reader if row and row[1].startswith("https://open.spotify.com/track/"))
    return 0

def is_live_track(title):
    """Check if the track title indicates a live version."""
    return 'live' in title.lower()

def refresh_token_and_retry(request_func, *args, **kwargs):
    """Refresh the access token and retry the request."""
    global access_token
    access_token = get_spotify_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    return request_func(*args, **kwargs, headers=headers)

# Global variables for rate limiting
class RateLimitState:
    def __init__(self):
        self.call_count = 0
        self.start_time = time.time()
        self.last_request_time = time.time()
        self.requests_per_window = 70  # Spotify's rate limit
        self.window_size = 30  # Window size in seconds

rate_state = RateLimitState()

def handle_rate_limiting():
    """Handles rate limiting for Spotify API calls with a sliding window."""
    current_time = time.time()
    
    # Reset counter if window has passed
    if current_time - rate_state.start_time >= rate_state.window_size:
        rate_state.call_count = 0
        rate_state.start_time = current_time
    
    rate_state.call_count += 1
    
    # If we've hit the rate limit
    if rate_state.call_count >= rate_state.requests_per_window:
        wait_time = rate_state.window_size - (current_time - rate_state.start_time)
        if wait_time > 0:
            print(f"\nRate limit reached. Waiting {wait_time:.1f} seconds...")
            # Show waiting progress
            for remaining in range(int(wait_time), 0, -1):
                print(f"Waiting... {remaining} seconds remaining", end='\r')
                time.sleep(1)
            print("\nResuming requests...")
            
        rate_state.call_count = 0
        rate_state.start_time = time.time()
    
    # Ensure minimum delay between requests
    time_since_last = current_time - rate_state.last_request_time
    if time_since_last < 0.05:  # Minimum 50ms between requests
        time.sleep(0.05 - time_since_last)
    
    rate_state.last_request_time = time.time()

def get_spotify_tracks(artist, title, headers):
    """Fetch tracks from Spotify with improved rate limiting and error handling."""
    endpoint = "https://api.spotify.com/v1/search"
    query = f"artist:\"{artist}\" track:\"{title}\""
    search_params = {"q": query, "type": "track", "limit": 50, "market": "PL"}
    
    max_retries = 5
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            handle_rate_limiting()  # Apply rate limiting before request
            
            response = requests.get(endpoint, headers=headers, params=search_params)
            
            # Handle token expiration
            if response.status_code == 401:
                print("Access token expired. Refreshing...")
                new_token = get_spotify_access_token()
                headers["Authorization"] = f"Bearer {new_token}"
                continue
            
            # Handle rate limiting from Spotify
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 30))
                print(f"\nSpotify rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            
            try:
                return response.json().get("tracks", {}).get("items", [])
            except json.JSONDecodeError as e:
                print(f"Invalid JSON response: {str(e)}")
                print(f"Response content: {response.text[:500]}...")
                return []
            
        except requests.exceptions.RequestException as e:
            wait_time = base_delay ** attempt
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print("Max retry attempts reached. Skipping track.")
                return []
    
    return []

# Token bucket rate limiter class
class SpotifyRateLimiter:
    def __init__(self, rate=1000, per=3600):
        self.rate = rate  # Number of requests allowed
        self.per = per    # Time period in seconds
        self.tokens = rate
        self.last_update = time.time()
    
    def update_tokens(self):
        now = time.time()
        time_passed = now - self.last_update
        self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per))
        self.last_update = now
    
    def acquire(self):
        self.update_tokens()
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False
    
    def wait_for_token(self):
        while not self.acquire():
            wait_time = (1 - self.tokens) * (self.per / self.rate)
            time.sleep(min(wait_time, 60))  # Cap maximum wait at 60 seconds

def initialize_results_file(filename):
    """Ensure the results CSV file exists with headers."""
    if not os.path.exists(filename):
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Year", "Track ID", "Track Name", "Artist ID", "Artist Name", "Album ID", "Popularity"])
        print(f"'{filename}' created with headers.")

def get_existing_tags():
    """Extract tags from existing CSV files in the current directory."""
    csv_files = [f for f in os.listdir() if f.endswith('.csv') and f not in ['artist.csv', 'results.csv']]
    return [os.path.splitext(f)[0] for f in csv_files]
def track_sort_key(track):
    """Define the sorting criteria for selecting the track."""
    # Check if the track has a single artist
    has_single_artist = len(track.get("artists", [])) == 1

    # Check if the track title contains a hyphen
    has_hyphen_in_title = "-" in track.get("name", "")

    # Extract release year and popularity for fallback sorting
    album = track.get("album", {})
    release_year = (
        int(album.get("release_date", "0000")[:4]) if album.get("release_date") else 0
    )
    popularity = track.get("popularity", 0)

    # Sorting order:
    # 1. Tracks with a single artist are prioritized (`True` > `False`).
    # 2. Tracks without a hyphen in the title are prioritized (`False` > `True` for `has_hyphen_in_title`).
    # 3. Fallback: Older tracks by release year, then higher popularity.
    return (not has_single_artist, has_hyphen_in_title, release_year, -popularity)


def main():
    global headers, access_token, api_call_count, call_start_time

    # Prompt for the number of tracks to retrieve
    while True:
        try:
            max_tracks = int(input("Please enter the number of Spotify tracks to retrieve: ").strip())
            if max_tracks <= 0:
                print("Please enter a positive integer.")
            else:
                break
        except ValueError:
            print("Invalid input. Please enter a valid number.")

    # Ensure the artist.csv exists and initialize it if needed
    initialize_results_file("results.csv")

    if os.path.exists(ARTIST_FILE):
        remove_file = input(f"'{ARTIST_FILE}' already exists. Do you want to remove it? (y/n): ").strip().lower()
        if remove_file == 'y':
            os.remove(ARTIST_FILE)
            print(f"'{ARTIST_FILE}' removed.")
            if os.path.exists("results.csv"):
                os.remove("results.csv")
                print("'results.csv' removed.")
            tags_input = input("Please enter the tags to search for (e.g., 'rock,pop'): ").strip()
            tags = tags_input.split(',')
            try:
                process_tags(tags)
            except Exception as e:
                print(f"Error in main (code1): {str(e)}")
        else:
            print(f"'{ARTIST_FILE}' will not be removed. Proceeding with existing data.")
            # Get tags from existing files when resuming
            tags = get_existing_tags()
            if not tags:
                print("Warning: No existing tag files found. This might affect the saving process.")
    else:
        print(f"'{ARTIST_FILE}' not found. Starting new data retrieval.")
        initialize_results_file("results.csv")
        tags_input = input("Please enter the tags to search for (e.g., 'rock,pop'): ").strip()
        tags = tags_input.split(',')
        try:
            process_tags(tags)
        except Exception as e:
            print(f"Error in main (code2): {str(e)}")

    if os.path.exists(ARTIST_FILE):
        with open(ARTIST_FILE, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames
            all_rows = list(reader)

        if "PROCESSED" not in fieldnames:
            fieldnames.append("PROCESSED")
            with open(ARTIST_FILE, "w", newline='', encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in all_rows:
                    row["PROCESSED"] = "No"
                    writer.writerow(row)

        # Initialize tracks_to_search here
        tracks_to_search = []
        with open(ARTIST_FILE, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get("PROCESSED", "No") == "No":
                    year = row.get("Year", "")
                    year = int(year) if year.isdigit() else year
                    isrc = row.get("ISRC", "")
                    tracks_to_search.append((row["Artist"], row["Title"], year, isrc))

        total_tracks = len(tracks_to_search)
        processed_count = get_retrieved_tracks_count("results.csv")
        total_tracks = min(len(tracks_to_search), max_tracks)
        processed_tracks = 0
        goal_tracks = max_tracks
        access_token = get_spotify_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        existing_track_ids = set()
        start_time = time.time()
        track_times = []
        api_call_count = 0
        call_start_time = time.time()

        # Now we can loop over tracks_to_search safely
        for artist, title, year, isrc in tracks_to_search:
            if processed_count + processed_tracks >= max_tracks:
                print(f"Reached target number of tracks ({max_tracks}). Stopping.")
                break

            start_track_time = time.time()
            processed_tracks += 1
            total_processed = processed_count + processed_tracks

            print(f"Processing track {total_processed}/{goal_tracks} ({total_processed/goal_tracks*100:.2f}%)")
            print(f"Current Track: Artist - {artist}, Title - {title}")

            retry_attempts = 5
            tracks = None
            for attempt in range(retry_attempts):
                try:
                    tracks = get_spotify_tracks(artist, title, headers)
                    if not tracks:
                        print(f"No results found for artist {artist}, track {title}.")
                        update_processed_status(ARTIST_FILE, title)
                        break

                    # Filter out live versions
                    exact_artist_tracks = [track for track in tracks if track["artists"][0]["name"].strip().lower() == artist.strip().lower()]
                    non_live_tracks = [track for track in exact_artist_tracks if not is_live_track(track["name"])]
                    live_tracks = [track for track in exact_artist_tracks if is_live_track(track["name"])]
                    break  # Success, exit retry loop

                except TypeError as e:
                    print(f"TypeError on attempt {attempt + 1}/{retry_attempts}: {str(e)}")
                    if attempt == retry_attempts - 1:
                        print("Max retry attempts reached. Skipping track.")
                        update_processed_status(ARTIST_FILE, title)
                        continue
                    else:
                        print(f"Retrying after error (attempt {attempt + 1}/{retry_attempts})...")
                        time.sleep(2 ** attempt)  # Exponential backoff on retries


            if not tracks:
                continue

            # Choose the track with the oldest release year and highest popularity
            selected_track = None
            if non_live_tracks:
                selected_track = min(non_live_tracks, key=track_sort_key)
            elif live_tracks:
                selected_track = min(live_tracks, key=track_sort_key)

            if not selected_track:
                print(f"No suitable tracks found for artist {artist}, track {title}.")
                update_processed_status(ARTIST_FILE, title)  # Mark as "No" if not found
                continue

            track_id = selected_track["id"]
            track_name = selected_track["name"]
            artist_id = selected_track["artists"][0]["id"]
            artist_name = selected_track["artists"][0]["name"]
            album_id = selected_track["album"]["id"]
            popularity = selected_track["popularity"]
            release_year = selected_track["album"]["release_date"][:4] if not year else year
            track_id_url = f"https://open.spotify.com/track/{track_id}"

            # Save Spotify results into separate files based on tags
            for tag in tags:
                tag_file = f"{tag.strip()}.csv"
                with open(tag_file, "a", newline="", encoding="utf-8") as tag_csvfile:
                    writer = csv.writer(tag_csvfile)
                    writer.writerow([int(release_year), track_id_url, track_name, artist_id, artist_name, album_id, popularity])

            # Append new tracks to results.csv without removing existing tracks
            if not remove_duplicate_tracks("results.csv", track_id_url):
                with open("results.csv", "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([int(release_year), track_id_url, track_name, artist_id, artist_name, album_id, popularity])
                existing_track_ids.add(track_id_url)

            # Update "PROCESSED" status to "Yes" when track is found
            update_processed_status(ARTIST_FILE, title, track_id)

            track_time = time.time() - start_track_time
            track_times.append(track_time)

            api_call_count += 1
            handle_rate_limiting()

            if track_times:
                avg_time_per_track = sum(track_times) / len(track_times)
                remaining_tracks = max_tracks - total_processed
                estimated_time_remaining = avg_time_per_track * remaining_tracks / 60

                print(f"Estimated time remaining: {estimated_time_remaining:.2f} minutes")



if __name__ == "__main__":
    main()