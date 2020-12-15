### necessary imports ###
import os
from pathlib import Path
from datetime import datetime, timedelta
from pandas import DataFrame 
import regex as re
import tweepy
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[0]

### lambda handler ###
def lambda_handler(event, context):
    print("---> get Twitter credentials")
    TWITconsumer_key = os.getenv("TWITCONSUMER_KEY")
    TWITconsumer_secret = os.getenv("TWITCONSUMER_SECRET")
    TWITaccess_token = os.getenv("TWITACCESS_TOKEN")
    TWITaccess_token_secret = os.getenv("TWITACCESS_TOKEN_SECRET")
    print("---> authenticate Twitter connection")
    auth = tweepy.OAuthHandler(TWITconsumer_key, TWITconsumer_secret)
    auth.set_access_token(TWITaccess_token, TWITaccess_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True)
    print("---> Twitter connected")

    tweets = {} # store all ids and tweets
    days = 1 # set the # of 'past' days to pull tweets from, end date
    today = datetime.utcnow() # get todays date
    end_date = today - timedelta(days=days) # <-- set the end date
    end_str = end_date.strftime('%m/%d/%Y') # set the end date as str
    start = datetime.now() # set the start date 

    ### search hashtags ###
    print('--- searching hashtags ---')
    tags = ['datascience', 'machinelearning', 'artificialintelligence']
    for tag in tags: # loop through hashtags
      try:
        print(f'---> hashtag: {tag}')
        for status in tweepy.Cursor(api.search,q=tag,
                                    since=end_str,   
                                    exclude_replies=True,    
                                    lang='en', 
                                    tweet_mode='extended').items(100):
          if status.full_text is not None:
            text = status.full_text.lower()

            id_s = status.id # store the tweet id 
            date = status.created_at # store the date created 
            name = status.user.name # store the user name 
            tweets[id_s] = [date, name, text] # add elements to dict
            
      ### handle errors ###
      except tweepy.TweepError as e: 
        print("Tweepy Error: {}".format(e))
 
    for key, val in tweets.items(): # loop through dictionary key/values
      val0, val1, val2 = val # unpack all variables 
      tags = re.findall("[#]\w+", val2) # get all words starting with '#'.
      tweets[key] = [val0, val1, val2, tags] # add elements to the dictionary

    print('--- hashtags tweets ---')
    print(f'tweets count: {len(tweets)}') # show tweets count
    
    print('--- timer ---')
    break1 = datetime.now()
    print("Elapsed time: {0}".format(break1-start)) # show timer
    
    ### create a dataframe for the pulled tweets ###
    print('--- remove any duplicates ---')
    df1 = DataFrame.from_dict(tweets, orient='index', columns=['date', 'name', 'text',  'tags']) # create a dataframe from for the tweets dict
    df1.reset_index(inplace=True) # reset the index 
    df1 = df1.rename(columns = {'index':'id'}) # rename columns 
    df1 = df1.drop_duplicates(subset=['id'], keep='last') # drop duplicates
    df1[['First','Last']] = df1.text.str.split(n=1, expand=True)  # split the text column into 2 
    df1 = df1.drop_duplicates(subset=['First']) # drop duplicates ofr First column
    df1 = df1.drop(columns=['First', 'Last']) # drop First and Last columns 
    strings = ['rt', '@', 'trial', 'free', 'register', 'subscription'] # list of substrings 
    df1 = df1[~df1.text.str.contains('|'.join(strings))] # remove any text values that contain a string from strings 

    print('--- pulled tweets dataframe ---')
    print(df1.dtypes) # show column data types 
    print(df1.shape) # show dataframe shape 
    print(df1.head()) # show dataframe
    print('--- pulled tweets text ---')
    print(df1['text'].values)  # show the dataframe text values 

    print("--- get AWS credentials ---")
    sql_AWS = os.getenv("AWSSQL")
    print("---> complete")

    ### push the final dataframe to the SQL database ###
    print("--- push to dataframe AWS database ---")
    engine = create_engine(sql_AWS) # create engine
    df1.to_sql('tweets_storage', con=engine, index=False, if_exists='append') # push the dataframe to the database
    print("---> complete")

    print('--- timer ---')
    break2 = datetime.now()
    print("Elapsed time: {0}".format(break2-start)) # show timer 
