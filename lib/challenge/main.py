import json
import emoji
import pandas as pd
import logging
import sys
import argparse
from pprint import pprint
from types import SimpleNamespace
import inspect
from collections import defaultdict

# harcoded input variables.
## to be replaced by kwargs* or argv, argc / simulate

def map_reduce_dataframe(dataframe, column_name, columns=['hashtag', 'count']):

    # Initialize a dictionary to store the intermediate word counts
    word_counts = defaultdict(int)
    
    # Iterate through each row in the DataFrame
    for index, row in dataframe.iterrows():
        # Access the array of tuples in the specified column
        array_of_tuples = row[column_name]
        
        # Iterate through each tuple in the array
        for e in array_of_tuples:
            key = e[columns[0]]
            value = e[columns[1]]
            # Increment the word count in the dictionary
            word_counts[key] += int(value)
    
    # Convert the dictionary to a DataFrame for the final summary
    summary_df = pd.DataFrame(list(word_counts.items()), columns=columns)
    
    return summary_df

"""
[ready]
1. Los top 10 tweets más retweeted
output = tweet_id, count_of_user_instances_id
logic = id, sum(retweenCount) order by 2 desc limit K

Args:
    df (dataframe): input dataframe

Returns:
    df (dataframe): with extra columns emoji_array
"""
def q1(df):
    _logger.debug(f" >> { inspect.stack()[0][3]}")
    df = df[['id','retweetCount']].reindex()
    df=df.groupby('id')['retweetCount'].agg(MySum='sum')
    df = df.sort_values(by='MySum', ascending=False)
    return df
    

"""
[ready]
2. Los top 10 users en función a la cantidad de tweets que emitieron.
output = user(username), count_of_tweet_id

assumption = unique tweets  -- no retweets
logic = user, count(id) order by 2 desc limit K

Args:
    df (dataframe): input dataframe

Returns:
    df (dataframe): with extra columns emoji_array
"""
def q2(df):
    _logger.debug(f" >> { inspect.stack()[0][3]}")
    df = df[['user','id']]
    df['username'] = df['user'].apply(lambda x : x['username'])
    df = df[['username','id']].reindex()
    df = df.groupby('username')['id'].agg(MyCount='count')
    df = df.sort_values(by='MyCount', ascending=False)
    return df

"""
[ready]
3. Los top 10 días donde hay más tweets. 
output = date, count_of_tweet_ids
assumption = unique tweets  -- no retweets
logic = date, count(id) order by 2 desc limit K

Args:
    df (dataframe): input dataframe

Returns:
    df (dataframe): with extra columns emoji_array
"""
def q3(df):
    _logger.debug(f" >> { inspect.stack()[0][3]}")
    df = df[df["date"]!=""]
    df = df[df["date"].notnull()]
    df['date_object'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H:%M:%S+00:00")
    df['date_day'] =df['date_object'].apply( lambda dt : dt.strftime("%Y-%m-%d"))
    df = df [['date_day','id']]
    df= df.groupby('date_day').count()
    return df


"""
4. Los top 10 hashtags más usados.
clean content whitespace, split into words, identify by containing [#]
not necessarily at beggining of word 

output = hashtag_string, count
assumption = unique tweets  -- no retweets

Args:
    df (dataframe): input dataframe

Returns:
    df (dataframe): with extra columns emoji_array
"""
def q4(df):
    _logger.debug(f" >> { inspect.stack()[0][3]}")
    def get_hashtags_array(text):
        """
        Extract hashtags from text

        Args:
            text (str): The input text to extract hashtags.

        Returns:
            Dictionary: lis of key value pairs {hashtag, coun=1}
        """
        response = list(text.replace("\n"," ").split(' '))
        d = []
        LIMIT = 10
        DEBUG = True
        for e in response:
            i=0
            e=e.strip()
            if '#' not in e:
                continue
            d.append({"hashtag":e, "count": 1})
            #d.append(({e: 1}))
            i=i+1
            if i > LIMIT and DEBUG:
                break
        return d
    df["hashtags"] = df["content"].apply(lambda tweet: get_hashtags_array(tweet))
    # make aggregations for hashtag
    result = map_reduce_dataframe(df, 'hashtags', columns=['hashtag','count'])
    result = result.sort_values(by='count', ascending=False)
    return result


"""
5. Los top 10 emojis más usados.
use emoji, analyze, retrieve values and store it as array in dataframe
use this intermediate output and rerout to q4() internal count rountine (mapreduce approach)
hex_value, count(*) order by 2 desc limit K

Args:
    df (dataframe): input dataframe

Returns:
    df (dataframe): with extra columns emoji_array
"""
def q5(df):
    _logger.debug(f" >> { inspect.stack()[0][3]}")
    def has_emoji(text):
        """
        Extra emojis from text

        Args:
            text (str): The input text to extract  for emojis.

        Returns:
            dictionary: list of key value pairs with emoji and count : 1
        """
        response = list(emoji.analyze(text.replace("\n","")))
        d = []
        for e in response:
            i=0
            if e is None:
                continue
            d.append(({"emoji":e.value.emoji, "count": 1}))
            i=i+1
            if i > 3:
                break

        return d
    df["emojis"] = df["content"].apply(lambda tweet: has_emoji(tweet))
    # make aggregations for hashtag
    result = map_reduce_dataframe(df, 'emojis', columns=['emoji','count'])
    result = result.sort_values(by='count', ascending=False)    
    return result


"""
6. Los top 10 users más influyentes en función de lo retweeted de sus tweets.
assumption = retween count only, not original tweet
user, count(id) where retweeted_count > 0

Args:
    df (dataframe): input dataframe

Returns:
    df (dataframe): aggregated results
"""
def q6(df):
    _logger.debug(f" >> { inspect.stack()[0][3]}")
    df = df.loc[df["retweetCount"]]
    df = df[['user','id']]
    df['username'] = df['user'].apply(lambda x : x['username'])
    df = df[['username','id']].reindex()
    df = df.groupby('username')['id'].agg(MyCount='count')
    df = df.sort_values(by='MyCount', ascending=False)
    return df


# read the file ~400 MB directly to ram
def load_file(raw_data_list, _logger):
    raw_data_list = []
    with open(path) as fp:
        i = 0 
        for line in fp.readlines():
            line = line.strip()
            raw_data = json.loads(line)
            raw_data_list.append(raw_data)    
            i=i+1
    _logger.debug(f"read {i} lines")
    _logger.debug(f"returning dataframe")
    return pd.DataFrame(raw_data_list)
# Testing input file
## a json per line, splitted by '\n' within the file

def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def run():
    args = SimpleNamespace()
    args.loglevel="DEBUG"
    setup_logging(args.loglevel)
    _logger = logging.getLogger(__name__)
    _logger.debug("Start :::")   
    #args = parse_args(args)
    path = "/Users/sebastian.pizarro/0code/0nexus/aa-dec/data/farmers-protest-tweets-2021-2-4.json"
    # init
    df=load_file(path, _logger)
    # apply
    df=load_file(path, _logger)
    q5(df).head(10)

if __name__=='__main__':
    run()