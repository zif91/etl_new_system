"""
Google Ads API client initialization.
"""
import os
import pathlib
from google.ads.googleads.client import GoogleAdsClient


def init_google_ads_client():
    """
    Initialize Google Ads client using YAML config file path from environment.
    Expects environment variable GOOGLE_ADS_CONFIG_PATH pointing to google-ads.yaml.
    """
    config_path = os.getenv('GOOGLE_ADS_CONFIG_PATH')
    if not config_path or not pathlib.Path(config_path).is_file():
        raise EnvironmentError('GOOGLE_ADS_CONFIG_PATH not set or file does not exist')
    client = GoogleAdsClient.load_from_storage(config_path)
    return client

if __name__ == '__main__':
    client = init_google_ads_client()
    print('Google Ads client initialized:', client)
