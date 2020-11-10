# necessary imports 
import os
from pathlib import Path
import tweepy # use to interact with twitter api
from datetime import datetime, timedelta
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


def lambda_handler1(event, context):
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
  
    ### create some storage ###
    tweet_ids = [] # store all tweet ids 
    tweets = {} # store all ids and tweets
    names = []
    ### set days and end date for searching ### 
    days = 10 # set the # of 'past' days to pull tweets from, end date
    end_date = datetime.utcnow() - timedelta(days=days)
    date_str = end_date.strftime('%m/%d/%Y')

    ### get tweet_ids for all hashtags in the hashtags list ###
    # tags = ['machinelearning']
    tags = ['artificialintelligence', 'machinelearning', 'python',
            'datascience', 'deeplearning', 'reinforcementlearning', 
            'ai', 'neuralnetwork'] # set all the 'hashtags' we are searching for in the tweets
    
    for tag in tags:
        for status in tweepy.Cursor(api.search,q=tag,
                                    exclude_replies=True,
                                    lang="en",
                                    since=date_str).items(30):
            
            if status.text is not None:
                if (not status.retweeted) and ('RT @' not in status.text):
                    id_s = status.id
                    if id_s not in tweet_ids:
                        tweet_ids.append(id_s)                 
                        if status.created_at < end_date: # if the created date is less than the end date of the post
                            break 
    #key = 1324641631778332674
    for key in tweet_ids: # loop through dictionary keys 'ids'
        status = api.get_status(key, tweet_mode='extended') # pull from the users get status 
        text = status.full_text.lower() # lower case all the text 
        name = status.user.name # store the user name 
        if text is not None:
            tweets[key] = [name, text] # add the text and key to the dictionary
            names.append(name)

    print('--- users ---')
    print(names)        
    print('--- hashtags ---')
    print(tags)
    print('--- pulled tweet ids ---')
    print(tweet_ids)
    # print('------ tweets dict -----')
    # print(tweets)

    ### create a dataframe for the pulled tweets ###
    df1 = DataFrame.from_dict(tweets, orient='index', columns=['name', 'tweet_text']) # create a dataframe from for the tweets dict
    df1.reset_index(inplace=True) # reset the index 
    df1 = df1.rename(columns = {'index':'tweet_id'}) # rename columns 
    df1 = df1.drop_duplicates(subset=['tweet_id'], keep='last') # drop duplicates
    df1[['First','Last']] = df1.tweet_text.str.split(n=1, expand=True) 
    df1 = df1.drop_duplicates(subset=['First'])
    df1 = df1.drop(columns=['First', 'Last'])
   
   ### add pulled tweets to the tweets_storage file ###
    df = pandas.read_csv('tweets_storage.csv') # read in the hard copy of the tweet_storage
    df_tweets = pandas.concat([df1, df]) # concat the dataframes together 
    df_tweets = df_tweets.drop_duplicates(subset=['tweet_id'], keep='last') # drop duplicates 
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
   


    return {"statusCode": 200, "tweet": 'hi'}