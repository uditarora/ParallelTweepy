import configparser
import tweepy
import os
import json
from .task_manager import TaskManager


def get_twohop_followers(user_ids, task_manager, apis):
    """
    Fetches the two-hop follower network of the given users.
    Two-hop follower network referes to the entire network of the
    followers of a user as well as the followers of those followers.
    """
    task_manager.get_followers(user_ids)
    task_manager.run_tasks(apis)

    all_followers = set()
    for followers_file in os.listdir(task_manager.follower_folder_path):
        if '.json' not in followers_file:
            continue
        all_followers.update(
            task_manager.get_all_followers(followers_file[:-5]))

    task_manager.get_followers(user_ids)
    task_manager.run_tasks(apis)


def get_authors(tweet_objects):
    """
    Takes in the list of tweet_objects and returns the list of authors
    of the tweets.
    """
    user_ids = []
    for tweet in tweet_objects:
        user_ids.append(tweet['user']['id_str'])
    return user_ids


def process_users(user_ids, user_ignore_list, task_manager, apis):
    """
    Processes the list of users and fetches the following data related
    to them:
        - Followers and followees of the users
        - Timelines of the users
    """

    # This can be used to fetch the two-hop follower network of a user
    # get_twohop_followers(user_ids, task_manager, apis)

    filtered_user_ids = [user_id for user_id in user_ids
                         if user_id not in user_ignore_list]

    task_manager.get_followers(filtered_user_ids)
    task_manager.run_tasks(apis)

    task_manager.get_followees(filtered_user_ids)
    task_manager.run_tasks(apis)

    task_manager.get_timelines(filtered_user_ids)
    task_manager.run_tasks(apis)


def process_tweets(tweet_ids, user_ignore_list, task_manager, apis):
    """
    Processes the list of tweets and fetches the following data related
    to them:
        - Tweet objects
        - Last 100 (max) retweets
        - Followers and followees of the authors of the tweets
        - Timelines of the authors of the tweets
    """
    task_manager.get_tweet_details(tweet_ids)
    task_manager.run_tasks(apis)

    tweet_objects = []
    tweet_details = []
    for tweet_details_file in os.listdir(
            task_manager.tweet_details_folder_path):
        if '.json' not in tweet_details_file:
            continue
        with open(task_manager.tweet_details_folder_path +
                  tweet_details_file) as f:
            obj = json.load(f)
            tweet_objects.append(obj)
            tweet_details.append((tweet_details_file[:-5],
                                 obj['user']['id_str']))

    filtered_user_ids = []
    filtered_tweet_ids = []

    for tweet_id, user_id in tweet_details:
        if user_id not in user_ignore_list:
            filtered_user_ids.append(user_id)
            filtered_tweet_ids.append(tweet_id)

    task_manager.get_retweets(filtered_tweet_ids)
    task_manager.run_tasks(apis)

    process_users(filtered_user_ids, user_ignore_list, task_manager, apis)


def create_api_objects():
    """
    Creates the api objects from the config file.
    """
    settings_file = "apikeys/apikeys.txt"
    # Read config settings
    config = configparser.ConfigParser()
    config.readfp(open(settings_file))

    # Create API objects for each of the API keys
    # 1-based indexing of config file
    start_idx = 1
    end_idx = 2
    num_api_keys = end_idx - start_idx + 1

    apis = []

    print("Creating api objects for {} API keys".format(num_api_keys))
    for api_idx in range(start_idx, end_idx + 1):
        consumer_key = config.get('API Keys ' + str(api_idx), 'API_KEY')
        consumer_secret = config.get('API Keys ' + str(api_idx), 'API_SECRET')
        access_token_key = config.get('API Keys ' + str(api_idx),
                                      'ACCESS_TOKEN')
        access_token_secret = config.get('API Keys ' + str(api_idx),
                                         'ACCESS_TOKEN_SECRET')

        # Connect to Twitter API
        try:
            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token_key, access_token_secret)
            api = tweepy.API(auth, wait_on_rate_limit=True)
        except Exception as e:
            print("Error while creating API object: " + str(e))
            continue
        else:
            apis.append(api)

    return apis


def run(user_ids, tweet_ids, curr_datetime, root_dir):
    """
    This run method assumes that data is periodically collected from Twitter
    and stored in folders ordered by timestamp of data collection.

    The task_manager scans the previously stored data, and saves the delta.
        - In the case of followers/followees, it stores the list of followers
          added/subtracted at each run.
        - In the case of timelines, it stores the new tweets by a user after
          the last fetched tweet from a user's timeline.

    Parameters:
        - user_ids (Twitter username/user_id): The list of users for which
          data needs to be collected.
        - tweet_ids: The list of tweets for which data needs to be collected.
        - curr_datetime (str): The current timestamp used to create the
          corresponding directory to store the Twitter data.
        - root_dir (str): The root directory path where all the timestamp
          folders are created.
    """
    print(" --- Collecting twitter data for {} tweets and {} users ---"
          .format(len(tweet_ids), len(user_ids)))

    apis = create_api_objects()

    base_folder_path = root_dir + '/'

    # Load list of users to ignore
    user_ignore_list = set()
    if os.path.exists(base_folder_path + 'user_ignore_list.txt'):
        with open(base_folder_path + 'user_ignore_list.txt') as f:
            for line in f:
                user_ignore_list.add(line.strip())

    twitter_folder_path = base_folder_path + curr_datetime + '/' + 'twitter/'

    if not os.path.exists(twitter_folder_path):
        os.makedirs(twitter_folder_path)

    task_manager = TaskManager(base_folder_path, twitter_folder_path)

    process_tweets(tweet_ids, user_ignore_list, task_manager, apis)
    process_users(user_ids, user_ignore_list, task_manager, apis)


if __name__ == "__main__":
    run()
