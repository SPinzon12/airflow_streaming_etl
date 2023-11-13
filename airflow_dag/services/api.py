import numpy as np
import pandas as pd
import requests


def create_df(anime_data):
    anime_list = []
    for anime in anime_data:
        anime_info = {
            'mal_id': anime['mal_id'],
            'title': anime['title'],
            'rank': anime['rank'],
            'score': anime['score'],
            'season': anime['season'],
            'year': anime['year'],
            'aired_from': anime['aired']['from'],
            'aired_to': anime['aired']['to'],
            'popularity': anime['popularity'],
            'favorites': anime['favorites'],
            'members': anime['members'],
            'episodes': anime['episodes'],
            'genre': get_genre(anime),
            'demographic': get_demographic(anime),
            'rating': anime['rating'],
            'status': anime['status'],
            'broadcast_day': anime['broadcast']['day'],
            'broadcast_time': anime['broadcast']['time'],
            'broadcast_timezone': anime['broadcast']['timezone'],
            'producer': get_producer(anime),
            'producer_id': get_producer_id(anime),
            'studio': get_studio(anime),
            'studio_id': get_studio_id(anime),
            'type': anime['type']
        }
        anime_list.append(anime_info)
    season_df = pd.DataFrame(anime_list)
    return season_df

def get_genre(anime):
    if anime['genres']:
        return anime['genres'][0]['name']
    return np.nan

def get_demographic(anime):
    if anime['demographics']:
        return anime['demographics'][0]['name']
    return np.nan

def get_producer(anime):
    if anime['producers']:
        return anime['producers'][0]['name']
    return np.nan

def get_producer_id(anime):
    if anime['producers']:
        return anime['producers'][0]['mal_id']
    return np.nan

def get_studio(anime):
    if anime['studios']:
        return anime['studios'][0]['name']
    return np.nan

def get_studio_id(anime):
    if anime['studios']:
        return anime['studios'][0]['mal_id']
    return np.nan

def extract_data_api():
    api_url = "https://api.jikan.moe/v4/seasons"
    response = requests.get(api_url)
    anime_df = pd.DataFrame()

    if response.status_code == 200:
        data = response.json()["data"]

        for entry in data:
            year = entry['year']
            seasons = entry['seasons']

            for season in seasons:
                params = {
                    'year': year,
                    'season': season
                }
                base_url = 'https://api.jikan.moe/v4/seasons/{year}/{season}'
                url = base_url.format(**params)

                try:
                    response = requests.get(url)

                    if response.status_code == 200:
                        anime_data = response.json().get("data", [])
                        season_df = create_df(anime_data)
                        print(f"Year: {year}, Season: {season}")
                        anime_df = pd.concat([anime_df, season_df], ignore_index=True)
                    else:
                        print(f"Error getting data for Year: {year}, Season: {season}")

                except requests.exceptions.RequestException as e:
                    print(f"Error in API request: {e}")

    else:
        print("Error getting the list of years and seasons.")
    return anime_df