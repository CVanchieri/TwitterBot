### necessary imports ###
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas
from pandas import DataFrame 
import regex as re
import tweepy
import psycopg2
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
    days = 3 # set the # of 'past' days to pull tweets from, end date
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
    df1['retweet'] = 'NO' # add a retweet column, set to 'NO'
    strings = ['rt', '@', 'trial', 'free', 'register', 'subscription'] # list of substrings 
    df1 = df1[~df1.text.str.contains('|'.join(strings))] # remove any text values that contain a string from strings 

    print('--- pulled tweets dataframe ---')
    print(df1.dtypes) # show column data types 
    print(df1.shape) # show dataframe shape 
    print(df1.head()) # show dataframe
    print('--- pulled tweets text ---')
    print(df1['text'].values)  # show the dataframe text values 

    print("--- authenticate AWS connection ---")
    AWSdatabase = os.getenv("AWSDATABASE")
    AWSuser = os.getenv("AWSUSER")
    AWSpassword = os.getenv("AWSPASSWORD")
    AWShost = os.getenv("AWSHOST")
    AWSport = os.getenv("AWSPORT")
    sql_AWS = os.getenv("AWSSQL")

    ## connect to AWS database ###
    connection = psycopg2.connect(database=AWSdatabase,
                                  user=AWSuser,
                                  password=AWSpassword,
                                  host=AWShost,
                                  port=AWSport)

    cur = connection.cursor()
    print("---> complete")

    ### pull the data from SQL ###
    print("--- pulling current AWS database ---")
    sql_select_Query = "select * from tweets_storage" # query all of database 
    cur.execute(sql_select_Query)
    records = cur.fetchall()
    df2 = DataFrame(records)  # set as dataframe 
    df2.columns = ['id', 'date', 'name', 'text', 'tags', 'retweet'] # label the columns 
    print("---> complete")
    print(" --- current tweets dataframe ---")
    print(df2.dtypes) # show the column types 
    print(df2.shape) # show the dataframe shape 
    print(df2.head()) # show the dataframe 

    print("--- drop current dataframe ---")
    cur.execute("""DROP TABLE tweets_storage""") # drop the current table 
    connection.commit()
    print("---> complete")

    ### concat the new and old dataframes ###
    df3 = pandas.concat([df1, df2], axis = 0) # merge the old and new dataframes
    df3 = df3.reset_index(drop=True) # reset the index 
    df3 = df3.drop_duplicates(subset=['id'], keep='last') # drop duplicates
    df3[['First','Last']] = df3.text.str.split(n=1, expand=True) # split the text value into 2 
    df3 = df3.drop_duplicates(subset=['First']) # drop duplicates 
    df3 = df3.drop(columns=['First', 'Last']) # remove First and Last columns 

    ## filter tweets that have not been retweeted ### 
    retweets = df3['retweet'] == 'NO' # create a database of only retweet column values 'NO'
    retweets = df3[retweets]

    ### get a random sample from the filtered tweets ###
    sample = retweets['id'].sample() # get random sample ID 
    sample = int(sample)
    api.retweet(sample) # retweet the ID 
    df3.loc[df3['id'] == sample,'retweet']='YES' # change the 'NO' to 'YES'
    df3 = df3.reset_index(drop=True) # reset index 
    print('--- tweets available for retweet ---')
    print(retweets.shape) # show the dataframe shape 
    print(retweets.head(20)) # show the dataframe
    print('--- tweet picked for retweet ---')
    print(sample) # show the random id 

    ### push the final dataframe to the SQL database ###
    print("--- push to dataframe AWS database ---")
    engine = create_engine(sql_AWS) # create engine
    df3.to_sql('tweets_storage', con=engine, index=False, if_exists='replace') # push the dataframe to the database
    print("---> complete")

    ### final tweets dataframe ###
    print("--- pulling final AWS database ---")
    sql_select_Query = "select * from tweets_storage"  # query the whole database 
    cur.execute(sql_select_Query)
    records = cur.fetchall()
    tweets_database = DataFrame(records) # set data as dataframe 
    tweets_database.columns = ['id', 'date', 'name', 'text',  'tags', 'retweet'] # set the column names 
    cur.close()

    print('--- final tweets data frame ---')
    print(tweets_database.shape) # show the dataframe shape 
    print(tweets_database.head(50)) # show the dataframe 

    print('--- timer ---')
    break2 = datetime.now()
    print("Elapsed time: {0}".format(break2-start)) # show timer 
