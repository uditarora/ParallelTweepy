# ParallelTweepy
A parallel task scheduler for Tweepy.

## What is this?
ParallelTweepy you to use multiple Twitter API keys to run Twitter data collection jobs using the Tweepy library in parallel.

## Why?
Twitter's rate limits make it difficult to collect a lot of data using a single API key. ParallelTweey enables you to enqueue data collection jobs using a easy to use module.

## How To Use?
- `task_manager.py` defines the Task Scheduler that lets you schedule and execute tasks.
- `main.py` contains a sample of the type of tasks you can create and schedule using the `TaskManager`.
- You will need to store your Twitter API keys as per the format provided in the `apikeys/apikeys.txt` file.
