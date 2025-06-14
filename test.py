from collections import namedtuple

cric_api_config = {
    "api_key": "d8b3f623-ef7e-4990-983d-3185557da8c5",
    "current_matches": "https://api.cricapi.com/v1/currentMatches",
    "series_list": "https://api.cricapi.com/v1/series",
    "matches_list": "https://api.cricapi.com/v1/matches",
    "player_list_api": "https://api.cricapi.com/v1/players"
}

api_config = namedtuple('Cric_Api_config',cric_api_config.keys())
config = api_config(**cric_api_config)

API_KEY = "d8b3f623-ef7e-4990-983d-3185557da8c5"
current_matches = "https://api.cricapi.com/v1/currentMatches?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&offset=0"
cric_score = (
    "https://api.cricapi.com/v1/cricScore?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5"
)
series_list = "https://api.cricapi.com/v1/series?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&offset=0"
matches_list = "https://api.cricapi.com/v1/matches?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&offset=0"
player_list_api = "https://api.cricapi.com/v1/players?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&offset=0"
player_info_api = "https://api.cricapi.com/v1/players_info?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5"
match_info_api = "https://api.cricapi.com/v1/match_info?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&id=02536431-2a0c-4071-b708-c0cf8ea794fa"
series_info_api = "https://api.cricapi.com/v1/series_info?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&id=af43f3b7-a0a7-495a-bb37-32da6de6db99"
fantasy_squad_api = "https://api.cricapi.com/v1/match_squad?apikey=d8b3f623-ef7e-4990-983d-3185557da8c5&id=02536431-2a0c-4071-b708-c0cf8ea794fa"