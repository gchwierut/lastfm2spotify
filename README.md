# lastfm2spotify

A Python script that retrieves music track data from Last.fm and Spotify APIs, organizing tracks by genre tags and saving the results in CSV format.

## Features

- Fetches top tracks by genre tags from Last.fm API
- Matches tracks with Spotify data for additional metadata
- Supports multiple genre tags processing
- Implements robust rate limiting and error handling
- Saves results in CSV format with detailed track information
- Handles API authentication and token refresh automatically
- Includes progress tracking and estimated completion time

## Prerequisites

- Python 3.x
- Required Python packages (install via pip):
  ```bash
  pip install requests pandas
  ```
- Last.fm API key
- Spotify API credentials (Client ID and Client Secret)

## Configuration

1. Replace the API credentials in the script:
```python
LASTFM_API_KEY = 'your_api_key'
SPOTIFY_CLIENT_ID = 'your_spotify_client_id'
SPOTIFY_CLIENT_SECRET = 'your_spotify_client_secret'
```

2. Optional: Adjust rate limiting parameters if needed:
```python
requests_per_window = 70  # Spotify's rate limit
window_size = 30  # Window size in seconds
```

## Usage

1. Run the script:
```bash
python main.py
```

2. Enter the number of Spotify tracks you want to retrieve when prompted

3. If starting a new data collection:
   - Enter comma-separated genre tags (e.g., 'rock,pop,jazz')
   - The script will create separate CSV files for each tag

4. If resuming existing data:
   - Choose whether to remove existing files
   - The script will continue from where it left off

## Output Files

- `artist.csv`: Master list of tracks with processing status
- `results.csv`: Combined results with Spotify metadata
- Individual tag files (e.g., `rock.csv`, `pop.csv`): Genre-specific track data

### CSV File Structure

Results CSV columns:
- Year
- Track ID (Spotify URL)
- Track Name
- Artist ID
- Artist Name
- Album ID
- Popularity

## Features In Detail

### Rate Limiting

- Implements token bucket algorithm for Spotify API
- Handles rate limits with exponential backoff
- Shows progress and estimated completion time

### Error Handling

- Retries on API failures with exponential backoff
- Handles token expiration automatically
- Validates JSON responses
- Recovers from network issues

### Data Processing

- Removes duplicate tracks
- Filters live versions (optional)
- Sorts tracks based on custom criteria
- Interleaves tracks from different tags
- Maintains processing status for resume capability

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Last.fm API
- Spotify Web API
- All contributors and maintainers

## Notes

- The script uses exponential backoff for API retries
- Recommended to run with stable internet connection
- Large datasets may take significant time to process
- Consider API rate limits when adjusting batch sizes
