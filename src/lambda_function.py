# necessary imports 
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas
from pandas import DataFrame 
import regex as re
import tweepy # use to interact with twitter api
import psycopg2
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[0]

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

    tweets_users = {} # store all ids and tweets
    tweets_hashtags = {} # store all ids and tweets
    days = 1 # set the # of 'past' days to pull tweets from, end date
    today = datetime.utcnow()
    end_date = today - timedelta(days=days) # <-- set the end date
    end_str = end_date.strftime('%m/%d/%Y')
    start = datetime.now()

    ### get tweet_ids for all users in user_names list ###
    print('--- searching usernames ---')
    user_names = ['geeksforgeeks', 'TDataScience', 'odsc', 'hackernoon', 
                  'DataSciNews', 'DataScienceCtrl', 'TensorFlow', 'GoogleAI',
                  'nordicinst', 'AndrewYNg', 'kdnuggets', 'BernardMarr', 
                  'KirkDBorne', 'hmason', 'drfeifei', 'NandoDF', 'ilyasut',
                  'GitHub', 'marcusborba', 'MikeTamir', 'jeremyphoward', 'odsc', 
                  'pythontrending', 'gvanrossum', 'PythonHub', 'analyticbridge', 
                  'demishassabis', 'eigensteve', 'MLWhiz', 'JeffDean', 'PyTorch', 
                  'OpenAI', 'harbrimah', 'rasangarocks', 'ml_india_', 'inm7_isn', 
                  'mcqubit']
    for name in user_names: # loop through user names 
      try:
        print(f'---> user: {name}')
        for status in tweepy.Cursor(api.user_timeline,
                                    screen_name=name, 
                                    since=end_str,   
                                    exclude_replies=True,             
                                    tweet_mode='extended').items(50):
          if status.full_text is not None:
            text = status.full_text.lower()
            strings = ['rt', '@', 'free trial', 'register', 'subscription']
            if any(string in text for string in strings) is False:
              key_words = ['artificialintelligence', 'artificial intelligence', 'machinelearning', 'machine learning', 'python',
              'datascience', 'data science', 'deeplearning', 'deep learning' 'reinforcement learning', 
              'reinforcementlearning','regression', 'classification', 'neuralnetwork', 'neural network']
              for word in key_words:
                if word in text:
                  id_s = status.id # store the tweet id 
                  date = status.created_at
                  name = status.user.name
                  tweets_users[id_s] = [date, name, text]
      ### handle errors ###
      except tweepy.TweepError as e: 
        print("Tweepy Error: {}".format(e))
 
    for key, val in tweets_users.items():
      val0, val1, val2 = val
      tags = re.findall("[#]\w+", val2)
      tweets_users[key] = [val0, val1, val2, tags]

    print('--- users tweets ---')
    print(f'tweets count: {len(tweets_users)}') 
    #print(tweets_users)
    
    print('--- timer ---')
    break1 = datetime.now()
    print("Elapsed time: {0}".format(break1-start))

    ### search hashtags ###
    print('--- searching hashtags ---')
    tags = ['artificialintelligence', 'machinelearning', 'python', 'datamining', 'datavisualization',
            'dataalaytics', 'predictivemodeling', 'datascience', 'deeplearning', 'reinforcementlearning', 
            'neuralnetwork']
    for tag in tags: # loop through user names 
      try:
        print(f'---> hashtag: {tag}')
        for status in tweepy.Cursor(api.search,q=tag,
                                    since=end_str,   
                                    exclude_replies=True,             
                                    tweet_mode='extended').items(100):
          if status.full_text is not None:
            text = status.full_text.lower()
            strings = ['rt', '@', 'trial', 'free', 'register', 'subscription']
            if any(string in text for string in strings) is False:
                    id_s = status.id # store the tweet id 
                    date = status.created_at
                    name = status.user.name
                    tweets_hashtags[id_s] = [date, name, text]
      ### handle errors ###
      except tweepy.TweepError as e: 
        print("Tweepy Error: {}".format(e))
 
    for key, val in tweets_hashtags.items():
      val0, val1, val2 = val
      tags = re.findall("[#]\w+", val2)
      tweets_hashtags[key] = [val0, val1, val2, tags]

    print('--- hashtags tweets ---')
    print(f'tweets count: {len(tweets_hashtags)}') 
    #print(tweets_hashtags)
    
    print('--- timer ---')
    break1 = datetime.now()
    print("Elapsed time: {0}".format(break1-start))
    
    ### merge dictionaries ###
    tweets = {**tweets_users, **tweets_hashtags}
    print('--- pulled tweets ---')
    print(f'tweets count: {len(tweets)}') 
    print(tweets)
    ### create a dataframe for the pulled tweets ###
    print('--- remove any duplicates ---')
    df1 = DataFrame.from_dict(tweets, orient='index', columns=['date', 'name', 'text',  'tags']) # create a dataframe from for the tweets dict
    df1.reset_index(inplace=True) # reset the index 
    df1 = df1.rename(columns = {'index':'id'}) # rename columns 
    df1 = df1.drop_duplicates(subset=['id'], keep='last') # drop duplicates
    df1[['First','Last']] = df1.text.str.split(n=1, expand=True) 
    df1 = df1.drop_duplicates(subset=['First'])
    df1 = df1.drop(columns=['First', 'Last'])
    df1['retweet'] = 'NO'
    print('--- pulled tweets dataframe ---')
    print(df1.dtypes)
    print(df1.shape)
    print(df1.head())
    print('--- pulled tweets text ---')
    print(df1['text'].values)

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
    sql_select_Query = "select * from tweets_storage"
    cur.execute(sql_select_Query)
    records = cur.fetchall()
    df2 = DataFrame(records)
    df2.columns = ['id', 'date', 'name', 'text', 'tags', 'retweet']
    print("---> complete")
    print(" --- current tweets dataframe ---")
    print(df2.dtypes)
    print(df2.shape)
    print(df2.head())

    print("--- drop current dataframe ---")
    cur.execute("""DROP TABLE tweets_storage""")
    connection.commit()
    #cur.close()
    print("---> complete")

    ### concat the new and old dataframes ###
    df3 = pandas.concat([df1, df2], axis = 0)
    df3 = df3.reset_index(drop=True) 
    df3 = df3.drop_duplicates(subset=['id'], keep='last') # drop duplicates
    df3[['First','Last']] = df3.text.str.split(n=1, expand=True) 
    df3 = df3.drop_duplicates(subset=['First'])
    df3 = df3.drop(columns=['First', 'Last'])

    ### filter tweets that have not been retweeted ### 
    retweets = df3['retweet'] == 'NO'
    retweets = df3[retweets]

    ### get a random sample from the filtered tweets ###
    sample = retweets['id'].sample()
    sample = int(sample)
    api.retweet(sample)
    df3.loc[df3['id'] == sample,'retweet']='YES'
    df3 = df3.reset_index(drop=True)
    print('--- tweets available for retweet ---')
    print(retweets.shape)
    print(retweets.head(20))
    print('--- tweet picked for retweet ---')
    print(sample)

    ### push the final dataframe to the SQL database ###
    print("--- push to dataframe AWS database ---")
    engine = create_engine(sql_AWS)
    df3.to_sql('tweets_storage', con=engine, index=False, if_exists='replace')
    print("---> complete")

    ### final tweets dataframe ###
    print("--- pulling final AWS database ---")
    sql_select_Query = "select * from tweets_storage"
    cur.execute(sql_select_Query)
    records = cur.fetchall()
    tweets_database = DataFrame(records)
    tweets_database.columns = ['id', 'date', 'name', 'text',  'tags', 'retweet']
    cur.close()

    print('--- final tweets data frame ---')
    print(tweets_database.shape)
    print(tweets_database.head(50))
    # print(tweets_database.retweet.values())

    print('--- timer ---')
    break2 = datetime.now()
    print("Elapsed time: {0}".format(break2-start))
