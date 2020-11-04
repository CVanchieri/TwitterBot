# necessary imports 
import os
import random
import json
from pathlib import Path
import tweepy # use to interact with twitter api
from tweepy import Cursor
import csv
from datetime import datetime, date, time, timedelta
import pandas
from pandas import DataFrame 
import numpy as np

ROOT = Path(__file__).resolve().parents[0]


def get_tweet(tweets_file, excluded_tweets=None):
    """Get tweet to post from CSV file"""

    with open(tweets_file) as csvfile:
        reader = csv.DictReader(csvfile)
        possible_tweets = [row["tweet"] for row in reader]

    if excluded_tweets:
        recent_tweets = [status_object.text for status_object in excluded_tweets]
        possible_tweets = [tweet for tweet in possible_tweets if tweet not in recent_tweets]

    selected_tweet = random.choice(possible_tweets)

    return selected_tweet


def lambda_handler(event, context):
    print("Get credentials")
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    access_token = os.getenv("ACCESS_TOKEN")
    access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

    print("Authenticate")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True)
  
    ### set the users to pull tweets from ###
    user_names = ['TensorFlow', 'PythonHub', 'kdnuggets', 'BernardMarr']
    # user_names = ['geeksforgeeks', 'TDataScience', 'odsc', 'hackernoon', 
    #               'DataSciNews', 'DataScienceCtrl', 'TensorFlow', 'GoogleAI', 
    #               'nordicinst', 'AndrewYNg', 'kdnuggets', 'BernardMarr', 
    #               'KirkDBorne', 'hmason', 'drfeifei', 'NandoDF', 'ilyasut',
    #               'GitHub', 'twschaller', 'marcusborba', 'MikeTamir', 
    #               'jeremyphoward', 'odsc', 'pythontrending', 'gvanrossum',
    #               'PythonHub', 'analyticbridge', 'DataScienceCtrl', 'demishassabis',
    #               'eigensteve', 'MLWhiz', 'JeffDean', 'PyTorch', 'OpenAI']
    tweet_ids = [] # store all tweet ids 
    tweets = {} # store all ids and tweets
    hashtags = []
    retweets = [] # store a list ids of the tweets retweeted
    days = 2 # set the # of 'past' days to pull tweets from, end date
    end_date = datetime.utcnow() - timedelta(days=days) # <-- set the end date


    for name in user_names: # loop through user names 
      print(f'USER --> {name}') # print the user name 
      for status in Cursor(api.user_timeline, screen_name=name, replies=False, tweet_mode='extended').items(): # pull from the users timeline with Cursor
      
        id_s = status.id # store the tweet id 
        tweet_ids.append(id_s) # add the tweet id to the tweet_ids dict

        if status.created_at < end_date: # if the created date is less than the end date of the post
            break 

    for key in tweet_ids: # loop through dictionary keys 'ids'
      status = api.get_status(key, tweet_mode='extended') # pull from the users get status 
      entities = status.entities
      user = status.user
      tags = ['datastructures','data structures', 'artificialintelligence', 'artificial intelligence', 'machinelearning', 'machine learning' 'python',
              'datascience', 'data science', 'deeplearning', 'deep learning' 'reinforcement learning', 
              'reinforcementlearning','regression', 'classification', 'neuralnetwork', 'neural network', 
              'data analysis', 'dataanalysis'] # set all the 'words' we are searching for in the tweets 
      if status.full_text is not None: # if the text is not empty
        text = status.full_text.lower() # lower case the text 
      
        # # loop through each tag 
        for tag in tags:
          if tag in text: # if the tag is found in the text 
            name = user.name
            tweets[key] = [name, text] # add the text and key to the dictionary
      
    print(f'# of tweet_ids pulled : {tweet_ids}') # show all the tweet_ids pulled
    print(f'# of tweets that pass filter: {len(tweets)}') # show the count # of tweets 

    df = pandas.read_csv('tweets_storage.csv')
    df1 = DataFrame.from_dict(tweets, orient='index', columns=['name', 'tweet_text']) # create a data from for the tweets dict
    df1.reset_index(inplace=True) # reset the index 
    df1 = df1.rename(columns = {'index':'tweet_id'}) # rename columns 
    df1 = df1.drop_duplicates(subset=['tweet_id', 'tweet_text'], keep='last')

    # hashtags = []
    # for idx, row in enumerate(df1.tweet_text.values):
    #   text = df1.tweet_text[idx]
    #   tags = list(filter(lambda word: word[0]=='#', df1.tweet_text[idx].split()))
    #   tags_all = ', '.join(tags)
    #   tags_all = tags_all.replace('.', '').replace('?', '').replace(':', '')
    #   hashtags.append(tags_all)
    # df1['hashtags'] = np.array(hashtags)

    df_tweets = pandas.concat([df1, df])
    df_tweets = df_tweets.drop_duplicates(subset=['tweet_id'], keep='last')
    hashtags = []
    for idx, row in enumerate(df_tweets.tweet_text.values):
      text = df_tweets.tweet_text[idx]
      tags = list(filter(lambda word: word[0]=='#', df_tweets.tweet_text[idx].split()))
      tags_all = ', '.join(tags)
      tags_all = tags_all.replace('.', '').replace('?', '').replace(':', '').replace(',,', ',')
      hashtags.append(tags_all)
    df_tweets['hashtags'] = np.array(hashtags)
    df_tweets.to_csv('tweets_storage.csv', index=False) # save a csv file of the tweets
    
    df2 = pandas.read_csv('retweets_storage.csv')
    retweets = []
    random_id = np.random.choice(df_tweets.tweet_id, replace=False)
    #random_id = df_tweets.sample()
    if random_id is not False:
      retweets.append(random_id)
      df_tweets = df_tweets[df_tweets['tweet_id'] != int(random_id)]
      print(random_id)
      #api.retweet(random_id) 
    df3 = DataFrame(retweets, columns=['tweet_id'])
    df_retweets = pandas.concat([df2, df3])
    df_retweets = df_retweets.sort_values('tweet_id')
    df_retweets = df_retweets.drop_duplicates(subset=['tweet_id'], keep='last')
    df_retweets.to_csv('retweets_storage.csv', index=False) # save a csv file of the tweets
    
    print('--- tweets pulled ---')
    print(list(df1.columns)) # show the columns of the dataframe 
    print(df1.shape) # show the shape of the dataframe
    print(df1.head(50)) # show the inital 10 rows of the dataframe
    print('--- tweets storage ---')
    print(list(df_tweets.columns)) # show the columns of the dataframe 
    print(df_tweets.shape) # show the shape of the dataframe
    print(df_tweets.head(50))
    print('--- retweets storage ---')
    print(list(df_retweets.columns)) # show the columns of the dataframe 
    print(df_retweets.shape) # show the shape of the dataframe
    print(df_retweets.head(50))
    # print('--- retweets ---')
    # print(list(df.columns)) # show the columns of the dataframe 
    # print(df2.shape) # show the shape of the dataframe
    # print(df2.head(10)) # show the inital 10 rows of the dataframe.

    # status = api.get_status(tweet_ids[1], tweet_mode='extended')
    # print(status)
    
    return {"statusCode": 200, "tweet": 'hi'}