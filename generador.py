import os
import networkx as nx 
import json 
import shutil,bz2,getopt,sys
from collections import defaultdict
import bz2
from datetime import datetime
import graphlib


def correct_filepath(path: str):
    if path.startswith('/') or path.startswith('\\'):
        path = path[1:]
    return path.replace('/', '\\').strip()

def process_tweets(input_directory: str, start_date, end_date, hashtags: list) -> list: 
    tweets = []
    for root, dirs, files in os.walk(input_directory):
        if dirs:
            for subdir in dirs:
                new_path = os.path.join(root, subdir)
                process_tweets(new_path, start_date, end_date, hashtags)
        for file in files:
            if file.endswith('.bz2'):
                with bz2.BZ2File(os.path.join(root, file), 'rb') as f:
                    for line in f:
                        try:
                            line = line.decode('utf-8')
                            tweet = json.loads(line)
                            created_at = tweet.get('created_at')
                            # Verifica si no hay restricciones de fecha ni hashtags
                            if not start_date and not end_date and not hashtags:
                                tweets.append(tweet)
                            else:
                                if not start_date and not end_date:
                                    # Si no hay restricciones de fecha, verifica los hashtags
                                    if hashtags and any(hashtag['text'] in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', [])):
                                        tweets.append(tweet)
                                elif created_at:
                                    # Si hay restricciones de fecha, verifica también la fecha y los hashtags
                                        tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
                                        if (start_date and tweet_date >= start_date) or (end_date and tweet_date <= end_date):
                                            if not hashtags or any(hashtag['text'] in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', [])):
                                                tweets.append(tweet)
                        except (json.JSONDecodeError, ValueError, TypeError) as e:
                            print(f"Error processing tweet: {e}")
    return tweets

def is_valid_tweet(tweet, start_date, end_date, hashtags):
    created_at = tweet.get('created_at')
    if not start_date and not end_date and not hashtags:
        return True
    if not start_date and not end_date:
        return hashtags and any(hashtag['text'] in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', []))
    if created_at:
        tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
        date_condition = (start_date and tweet_date >= start_date) or (end_date and tweet_date <= end_date)
        hashtag_condition = not hashtags or any(hashtag['text'] in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', []))
        return date_condition and hashtag_condition
    return False

def process_directory(directory, start_date, end_date, hashtags, tweets):
    for root, dirs, files in os.walk(directory):
        for subdir in dirs:
            new_path = os.path.join(root, subdir)
            process_directory(new_path, start_date, end_date, hashtags, tweets)
        for file in files:
            if file.endswith('.bz2'):
                process_bz2_file(os.path.join(root, file), start_date, end_date, hashtags, tweets)

def process_bz2_file(file_path, start_date, end_date, hashtags, tweets):
    with bz2.BZ2File(file_path, 'rb') as f:
        for line in f:
            try:
                line = line.decode('utf-8')
                tweet = json.loads(line)
                if is_valid_tweet(tweet, start_date, end_date, hashtags):
                    tweets.append(tweet)
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                print(f"Error processing tweet: {e}")

def process_tweets2(input_directory: str, start_date, end_date, hashtags: list) -> list:
    tweets = []
    process_directory(input_directory, start_date, end_date, hashtags, tweets)
    return tweets

def generate_graph_rt(tweets: list):
    G = nx.DiGraph()
    for tweet in tweets:
        try:
            tweet_rt = tweet.get('retweeted_status')
            if tweet_rt:
                retweeting_user = tweet['user']['screen_name']
                retweeted_user = tweet_rt['user']['screen_name']
                G.add_edge(retweeted_user, retweeting_user)
        except (KeyError, TypeError) as e:
            print(f"Error processing tweet: {e}")
    nx.write_gexf(G, 'retweet_graph.gexf')

def create_retweet_json(tweets: list):
    retweets = {}
    for tweet in tweets:
        retweeted_status = tweet.get('retweeted_status')
        if retweeted_status:
            retweeting_user = tweet["user"]["screen_name"]
            retweeted_user = retweeted_status["user"]["screen_name"]
            tweet_id = retweeted_status["id"]
            tweet_id = f'tweetId: {tweet_id}'
            if retweeted_user not in retweets:
                retweets[retweeted_user] = {
                    'receivedRetweets': 0,
                    'tweets': {}
                }

            retweet_data = retweets[retweeted_user]
            if tweet_id not in retweet_data['tweets']:
                retweet_data['tweets'][tweet_id] = {'retweetedBy': [retweeting_user]}
                retweet_data['receivedRetweets'] += 1
            else:
                retweet_data['tweets'][tweet_id]['retweetedBy'].append(retweeting_user)
                retweet_data['receivedRetweets'] += 1
            
    sorted_retweets = sorted(retweets.items(), key=lambda x: x[1]['receivedRetweets'], reverse=True)
    result = {"retweets": [{'username': key, **value} for key, value in sorted_retweets]}
    with open('rt8.json', 'w') as f:
        json.dump(result, f, indent=4)

def main(argv):
    input_directory = '/data'
    start_date = False
    end_date = False
    hashtags = []
    
    args = argv.split()
    opts = []
    
    i = 0
    while i < len(args):
        argumento = args[i]
        valor = args[i + 1] if i + 1 < len(args) else ''
        if argumento.startswith('--'):
            opts.append((argumento, ''))
        else:
            if argumento.startswith('-') and not valor.startswith('-'):
                opts.append((argumento, valor))
                i += 2
                continue
            elif argumento.startswith('-') and valor.startswith('-') and not valor.startswith('--'):
                pass
        i += 1
    
    for opt, arg in opts:
        if opt == '-d':
            input_directory = arg
        if opt == '-ff':
            end_date = datetime.strptime(arg, "%d-%m-%y")
        if opt == '-fi':
            start_date = datetime.strptime(arg, "%d-%m-%y")
        if opt == '-h':
            with open(arg, 'r') as file:
                hashtags = [line.strip() for line in file]
    
    tweets = process_tweets2(input_directory, start_date, end_date, hashtags)
    print('tweet procesados: ' + str(len(tweets)))
    
    for opt, arg in opts:
        if opt == '--grt':
            generate_graph_rt(tweets)
        if opt == '--jrt':
            create_retweet_json(tweets)
        if opt == '--gm':
            pass
            #generate_graph_mention()
        if opt == '--jm':
            pass
            #generate_json_mention()
        if opt == '--gcrt':
            pass
            #generate_graph_corretweet()
        if opt == '--jcrt':
            pass
            #generate_json_corretweet()

main("-d  data/2016/01/06/00 -fi 06-01-16 --jrt --grt")
