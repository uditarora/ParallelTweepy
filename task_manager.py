from multiprocessing import Process, Queue, current_process
from enum import Enum
from tqdm import tqdm
from collections import defaultdict
import json
import tweepy
import os
import queue


class TaskType(Enum):
    """
    This defines the types of tasks that the TaskManager can perform.
    """
    tweet_details = 0
    retweets = 1
    followers = 2
    twohup_followers = 3
    followees = 4
    timeline = 5


class TaskManager:
    """
    The TaskManager allows scheduling of different type of Twitter data
    tasks in a queue, which are executed in parallel by multiple processes.

    Instance Variables:
    - The tasks_pending queue stores all the pending tasks in a FIFO queue.
    - The tasks_pending_dict is a dictionary that stores the set of tasks
    pending corresponding to each task type.
    - The folder paths corresponding to where the different types of
    information will be stored are also defined.
    """

    def __init__(self, base_folder_path, twitter_folder_path, **args):
        self.base_folder_path = base_folder_path
        self.twitter_folder_path = twitter_folder_path

        self.timeline_folder_path = twitter_folder_path + 'timelines/'
        if not os.path.exists(self.timeline_folder_path):
            os.makedirs(self.timeline_folder_path)

        self.follower_folder_path = twitter_folder_path + 'followers/'
        if not os.path.exists(self.follower_folder_path):
            os.makedirs(self.follower_folder_path)

        self.followee_folder_path = twitter_folder_path + 'followees/'
        if not os.path.exists(self.followee_folder_path):
            os.makedirs(self.followee_folder_path)

        self.tweet_details_folder_path = twitter_folder_path + 'tweet_details/'
        if not os.path.exists(self.tweet_details_folder_path):
            os.makedirs(self.tweet_details_folder_path)

        self.retweets_folder_path = twitter_folder_path + 'retweets/'
        if not os.path.exists(self.retweets_folder_path):
            os.makedirs(self.retweets_folder_path)

        self.tasks_pending = Queue()
        self.tasks_pending_dict = defaultdict(set)

    def do_task(self, api):
        """
        Queries the tasks_pending queue to fetch a new task if available,
        and executes the tasks based on the TaskType.
        """
        while True:
            try:
                object_id, task_type = self.tasks_pending.get_nowait()
            except queue.Empty:
                break
            else:
                try:
                    if task_type == TaskType.tweet_details:
                        self._get_tweet_details(object_id, api)
                    elif task_type == TaskType.retweets:
                        self._get_retweets(object_id, api)
                    elif task_type == TaskType.followers:
                        self._get_followers(object_id, api)
                    elif task_type == TaskType.followees:
                        self._get_followees(object_id, api)
                    elif task_type == TaskType.timeline:
                        self._get_timelines(object_id, api)
                except Exception as e:
                    print("\nError: Unable to complete " + str(task_type) +
                          " for id: " + str(object_id) + " - " + str(e) + '\n')
                    continue
                finally:
                    print("\nProcessed: " + str(task_type) + " for id " +
                          str(object_id) + " is processed by " +
                          current_process().name + ".\nTasks left: " +
                          str(self.tasks_pending.qsize()) + '\n')
                    if object_id in self.tasks_pending_dict[task_type]:
                        self.tasks_pending_dict[task_type].remove(object_id)
        return True

    def run_tasks(self, apis):
        """
        Create processes for parallel execution - each process will use
        one API key to accomplish one task at a time.
        """
        processes = []
        for idx in range(len(apis)):
            current_api = apis[idx]
            # api_object = dill.dumps(current_api)
            p = Process(target=self.do_task, args=(current_api,))
            processes.append(p)
            p.start()

        # Avoiding deadlock
        for idx, p in enumerate(processes):
            while True:
                running = p.is_alive()
                if not self.tasks_pending.empty():
                    self.do_task(apis[idx])
                else:
                    if not running:
                        break

        # Completing processes
        print("Waiting for processes to finish...")
        for p in processes:
            p.join()

    def _get_tweet_details(self, tweet_id, api):
        print("Getting tweet details of tweet {}".format(tweet_id))

        tweet_details = api.get_status(tweet_id)

        print("Writing the details of {} to file...".format(tweet_id))

        with open(self.tweet_details_folder_path + '/' + str(tweet_id) +
                  '.json', 'w') as fw:
            json.dump(tweet_details._json, fw)
        return tweet_details

    def _get_retweets(self, tweet_id, api):
        print("Getting retweets of tweet {}".format(tweet_id))

        retweets = api.retweets(tweet_id, 200)

        print("Writing the {0} retweets of {1} to file".format(
            len(retweets), tweet_id))

        retweets_arr = []
        for retweet in retweets:
            retweets_arr.append(json.dumps(retweet._json))

        with open(self.retweets_folder_path + str(tweet_id) +
                  '.json', 'w') as fw:
            json.dump(retweets_arr, fw)

    def _get_followers(self, user_id, api):
        user_obj = api.get_user(user_id)

        if self.add_user_to_ignore_list(user_obj):
            return

        user_id = user_obj.id_str
        all_followers = self.get_all_followers(user_id)

        followers_added = []
        followers_subtracted = []
        followers_current = set()

        print("Getting followers of user {}".format(user_id))

        try:
            for follower in tqdm(tweepy.Cursor(
                    api.followers_ids, id=user_id).items(), unit="followers"):
                followers_current.add(follower)
        except Exception as e:
            print("Error while fetching user followers: " + str(e))
        else:
            followers_added = [item for item in followers_current
                               if item not in all_followers]
            followers_subtracted = [item for item in all_followers
                                    if item not in followers_current]
            followers = {'followers_added': followers_added,
                         'followers_subtracted': followers_subtracted}

            print("Writing followers for user {}. Added: {}, Subtracted: {}"
                  .format(user_id, len(followers_added),
                          len(followers_subtracted)))

            with open(self.follower_folder_path + str(user_id) +
                      '.json', 'w') as fw:
                json.dump(followers, fw)

    def _get_followees(self, user_id, api):
        user_obj = api.get_user(user_id)

        if self.add_user_to_ignore_list(user_obj):
            return

        user_id = user_obj.id_str
        all_followees = self.get_all_followees(user_id)

        followees_added = []
        followees_subtracted = []
        followees_current = set()

        print("Getting followees of user {}".format(user_id))

        try:
            for followee in tqdm(tweepy.Cursor(
                    api.friends_ids, id=user_id).items(), unit="followees"):
                followees_current.add(followee)
        except Exception as e:
            print("Error while fetching user followees: " + str(e))
        else:
            followees_added = [item for item in followees_current
                               if item not in all_followees]
            followees_subtracted = [item for item in all_followees
                                    if item not in followees_current]
            followees = {'followees_added': followees_added,
                         'followees_subtracted': followees_subtracted}

            print("Writing followees for user {}. Added: {}, Subtracted: {}"
                  .format(user_id, len(followees_added),
                          len(followees_subtracted)))

            with open(self.followee_folder_path + str(user_id) +
                      '.json', 'w') as fw:
                json.dump(followees, fw)

    def _get_timelines(self, user_id, api):
        user_obj = api.get_user(user_id)

        if self.add_user_to_ignore_list(user_obj):
            return

        user_id = user_obj.id_str
        last_tweet_id = self.get_last_tweet_id(user_id)

        tweets_arr = []

        print("Fetching timelines for user {}".format(user_id))

        try:
            if last_tweet_id != -1:
                for tweet in tqdm(tweepy.Cursor(
                        api.user_timeline, id=user_id,
                        since_id=int(last_tweet_id))
                        .items(), unit="tweets"):
                    tweets_arr.append(json.dumps(tweet._json))
            else:
                for tweet in tweepy.Cursor(
                        api.user_timeline, id=user_id).items():
                    tweets_arr.append(json.dumps(tweet._json))
        except Exception as e:
            print("Error while fetching user timeline: " + str(e))
        else:
            print("Writing {} tweets of user {}"
                  .format(len(tweets_arr), user_id))
            if (len(tweets_arr) != 0):
                with open(self.timeline_folder_path + str(user_id) +
                          '.json', 'w') as fw:
                    json.dump(tweets_arr, fw)

    def get_tweet_details(self, tweet_ids):
        for tweet_id in tweet_ids:
            if not os.path.exists(self.tweet_details_folder_path +
                                  str(tweet_id) + '.json') \
                    and tweet_id not in \
                    self.tasks_pending_dict[TaskType.tweet_details]:
                self.tasks_pending_dict[TaskType.tweet_details].add(tweet_id)
                self.tasks_pending.put((tweet_id, TaskType.tweet_details))

    def get_retweets(self, tweet_ids):
        for tweet_id in tweet_ids:
            if not os.path.exists(self.retweets_folder_path +
                                  str(tweet_id) + ".json") \
                    and tweet_id not in \
                    self.tasks_pending_dict[TaskType.retweets]:
                self.tasks_pending_dict[TaskType.retweets].add(tweet_id)
                self.tasks_pending.put((tweet_id, TaskType.retweets))

    def get_followers(self, user_ids):
        for user_id in user_ids:
            if not os.path.exists(self.follower_folder_path +
                                  str(user_id) + ".json") \
                    and user_id not in \
                    self.tasks_pending_dict[TaskType.followers]:
                self.tasks_pending_dict[TaskType.followers].add(user_id)
                self.tasks_pending.put((user_id, TaskType.followers))

    def get_followees(self, user_ids):
        for user_id in user_ids:
            if not os.path.exists(self.followee_folder_path +
                                  str(user_id) + ".json") \
                    and user_id not in \
                    self.tasks_pending_dict[TaskType.followees]:
                self.tasks_pending_dict[TaskType.followees].add(user_id)
                self.tasks_pending.put((user_id, TaskType.followees))

    def get_timelines(self, user_ids):
        for user_id in user_ids:
            if not os.path.exists(self.timeline_folder_path +
                                  str(user_id) + ".json") \
                    and user_id not in \
                    self.tasks_pending_dict[TaskType.timeline]:
                self.tasks_pending_dict[TaskType.timeline].add(user_id)
                self.tasks_pending.put((user_id, TaskType.timeline))

    def get_all_followers(self, user_id):
        """
        Parses through the already fetched followers at previous timesteps
        to find the complete list of followers of a user.
        """
        all_followers = set()

        time_folder_list = sorted(os.listdir(self.base_folder_path),
                                  reverse=True)

        for time_folder in time_folder_list:
            if '.txt' in time_folder:
                continue
            followers_folder = self.base_folder_path + time_folder + \
                '/twitter/followers/'
            followers_file = followers_folder + str(user_id) + '.json'
            if os.path.exists(followers_file):
                print("Existing file found for user" + str(user_id) +
                      "in folder " + str(time_folder))
                with open(followers_file) as f:
                    data = json.load(f)
                    all_followers.update(data['followers_added'])
                    for item in data['followers_subtracted']:
                        if item in all_followers:
                            all_followers.remove(item)

        return all_followers

    def get_all_followees(self, user_id):
        """
        Parses through the already fetched followees at previous timesteps
        to find the complete list of followees of a user.
        """
        all_followees = set()

        time_folder_list = sorted(os.listdir(self.base_folder_path),
                                  reverse=True)

        for time_folder in time_folder_list:
            if '.txt' in time_folder:
                continue
            followees_folder = self.base_folder_path + time_folder + \
                '/twitter/followees/'
            followees_file = followees_folder + str(user_id) + '.json'
            if os.path.exists(followees_file):
                print("Existing file found for user" + str(user_id) +
                      "in folder " + str(time_folder))
                with open(followees_file) as f:
                    data = json.load(f)
                    all_followees.update(data['followees_added'])
                    for item in data['followees_subtracted']:
                        if item in all_followees:
                            all_followees.remove(item)

        return all_followees

    def get_last_tweet_id(self, user_id):
        """
        Parses through the already fetched timelines at previous timesteps
        to find the last tweet_id fetched from a user's timeline.
        """
        time_folder_list = sorted(os.listdir(self.base_folder_path),
                                  reverse=True)

        for time_folder in time_folder_list:
            if '.txt' in time_folder:
                continue
            timelines_folder = self.base_folder_path + time_folder + \
                '/twitter/timelines/'
            timelines_file = timelines_folder + str(user_id) + '.json'
            if os.path.exists(timelines_file):
                print("Timeline file found for user" + str(user_id) +
                      "in folder " + str(time_folder))
                with open(timelines_file) as f:
                    data = json.load(f)
                    last_tweet = json.loads(data[0])
                    last_tweet_id = int(last_tweet["id"])
                    return last_tweet_id

        return -1

    def add_user_to_ignore_list(self, user_obj):
        """
        An user-level ignore list is maintined so that celebrity like users
        are not processed, thereby avoiding exceedance of Twitter API rate
        limits quickly.
        """
        if user_obj.followers_count > 20000 or user_obj.friends_count > 20000:
            print("IgnoreList: The user has more than 20000 " +
                  "followers/followees, ignoring.")
            with open(self.base_folder_path +
                      'user_ignore_list.txt', 'a+') as fw:
                fw.write(str(user_obj.id_str) + '\n')
            return True
        return False
