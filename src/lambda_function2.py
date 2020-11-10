# necessary imports 
import os
from pathlib import Path
import tweepy # use to interact with twitter api
from datetime import datetime,timedelta
import pandas
from pandas import DataFrame 

ROOT = Path(__file__).resolve().parents[0]


def get_tweet(tweets_file, excluded_tweets=None):
    """Get tweet to post from CSV file"""

    # with open(tweets_file) as csvfile:
    #     reader = csv.DictReader(csvfile)
    #     possible_tweets = [row["tweet"] for row in reader]

    # if excluded_tweets:
    #     recent_tweets = [status_object.text for status_object in excluded_tweets]
    #     possible_tweets = [tweet for tweet in possible_tweets if tweet not in recent_tweets]

    # selected_tweet = random.choice(possible_tweets)

    # return selected_tweet


def lambda_handler2(event, context):
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

    tweet_ids = [] # store all tweet ids 
    tweets = {} # store all ids and tweets
    names = []
    retweets = {} # store a list ids of the tweets retweeted
    days = 2 # set the # of 'past' days to pull tweets from, end date
    end_date = datetime.utcnow() - timedelta(days=days) # <-- set the end date
  
    ### set the users to pull tweets from ###
    #user_names = ['TensorFlow', 'PythonHub', 'kdnuggets', 'BernardMarr']
    user_names = ['geeksforgeeks', 'TDataScience', 'odsc', 'hackernoon', 
                  'DataSciNews', 'DataScienceCtrl', 'TensorFlow', 'GoogleAI', 
                  'nordicinst', 'AndrewYNg', 'kdnuggets', 'BernardMarr', 
                  'KirkDBorne', 'hmason', 'drfeifei', 'NandoDF', 'ilyasut',
                  'GitHub', 'twschaller', 'marcusborba', 'MikeTamir', 
                  'jeremyphoward', 'odsc', 'pythontrending', 'gvanrossum',
                  'PythonHub', 'analyticbridge', 'DataScienceCtrl', 'demishassabis',
                  'eigensteve', 'MLWhiz', 'JeffDean', 'PyTorch', 'OpenAI']

    ### get tweet_ids for all users in user_names list ###
    for name in user_names: # loop through user names 
      for status in tweepy.Cursor(api.user_timeline, 
                          screen_name=name, 
                          replies=False, 
                          tweet_mode='extended').items(20): # pull from the users timeline with Cursor
        id_s = status.id # store the tweet id 
        tweet_ids.append(id_s) # add the tweet id to the tweet_ids dict
        names.append(name)

        if status.created_at < end_date: # if the created date is less than the end date of the post
            break 

    for key in tweet_ids: # loop through dictionary keys 'ids'
      status = api.get_status(key, tweet_mode='extended') # pull from the users get status 
      tags = ['artificialintelligence', 'artificial intelligence', 'machinelearning', 'machine learning', 'python',
              'datascience', 'data science', 'deeplearning', 'deep learning' 'reinforcement learning', 
              'reinforcementlearning','regression', 'classification', 'neuralnetwork', 'neural network', 
              ] # set all the 'words' we are searching for in the tweets 
      if status.full_text is not None: # if the text is not empty
        text = status.full_text.lower() # lower case all the text 
      
        for tag in tags: # loop through each tag 
          if tag in text: # if the tag is found in the text 
            name = status.user.name # store the user name 
            tweets[key] = [name, text] # add the text and key to the dictionary
    
    print('--- users ---')
    print(names)        
    print('--- hashtags ---')
    print(user_names) 
    print('--- pulled tweet ids ---')
    print(tweet_ids)

    ### create a dataframe for the pulled tweets ###
    df1 = DataFrame.from_dict(tweets, orient='index', columns=['name', 'tweet_text']) # create a dataframe from for the tweets dict
    df1.reset_index(inplace=True) # reset the index 
    df1 = df1.rename(columns = {'index':'tweet_id'}) # rename columns 
    df1 = df1.drop_duplicates(subset=['tweet_id', 'tweet_text'], keep='last') # drop duplicates

    ### add pulled tweets to the tweets_storage file ###
    df = pandas.read_csv('tweets_storage.csv') # read in the hard copy of the tweet_storage
    df_tweets = pandas.concat([df1, df]) # concat the dataframes together 
    df_tweets = df_tweets.drop_duplicates(subset=['tweet_id', 'tweet_text'], keep='last') # drop duplicates 
    df_tweets[['First','Last']] = df_tweets.tweet_text.str.split(n=1, expand=True) 
    df_tweets = df_tweets.drop_duplicates(subset=['First'])
    df_tweets = df_tweets.drop(['First', 'Last'], axis=1)
    df_tweets.to_csv('tweets_storage.csv', index=False) # save a csv file of the tweets
  
    print('--- # of tweets that pass filter ---') 
    print(df1.shape[0])
    print('--- pulled tweets dataframe ---')
    print(df1.shape)
    print(df1.head(20))
    print('--- tweets storage dataframe ---')
    print(df_tweets.shape)
    print(df_tweets.head(20))










    ### retweet a random tweet_id from the df_tweets dataframe ### 
    random_choice = df_tweets.sample()
    random_id = random_choice.iloc[0]['tweet_id']
    #api.retweet(random_id) 
    print('--- random id chosen ---')
    print(random_id)
    print('--- random tweet retweeted ---')
    print(random_choice)

    ### add the retweet random_id data to the retweets_storage dataframe ###
    df2 = pandas.read_csv('retweets_storage.csv')
    if random_id not in df2.tweet_id: # if the random_id is not in the df2 dataframe
      retweets[random_id] = [random_choice.iloc[0]['name'], random_choice.iloc[0]['tweet_text']] # add the random id and data tot he retweets dict
      df_tweets = df_tweets[df_tweets['tweet_id'] != int(random_id)] # remove the tweet with the random_id 
      df3 = DataFrame.from_dict(retweets, orient='index', columns=['name', 'tweet_text']) # create a dataframe from for the tweets dict
      df3.reset_index(inplace=True) # reset the index 
      df3 = df3.rename(columns = {'index':'tweet_id'}) # rename the index to tweet_id
      df_retweets = pandas.concat([df2, df3]) # concat the dataframes together 
      df_retweets = df_retweets.sort_values('tweet_id') # sort the data on tweet_id
      df_retweets = df_retweets.drop_duplicates(subset=['tweet_id', 'tweet_text'], keep='last') # remove duplicates
      print('--- retweets storage ---')
      print(df_retweets.shape)
      print(df_retweets.head(50))
      df_retweets.to_csv('retweets_storage.csv', index=False) # save a csv file of the tweets
  
    return {"statusCode": 200, "tweet": 'hi'}