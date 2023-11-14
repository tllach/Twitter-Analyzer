import os
import networkx as nx 
import glob 
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

def generate_graph_rt(tweets: list):
    G = nx.DiGraph()
    for tweet in tweets:
        try:
            tweet_rt = tweet.get('retweeted_status')
            if tweet_rt:
                retweeted_user = tweet['user']['screen_name']
                retweeting_user = tweet_rt['user']['screen_name']
                G.add_edge(retweeted_user, retweeting_user)
        except (KeyError, TypeError) as e:
            print(f"Error processing tweet: {e}")

    nx.write_gexf(G, 'retweet_graph.gexf')


def create_retweet_json(tweets: list):
    rt = [{'username': "", 
           'receivedRetweets': 0, 
           'tweets': {}
        }]
    
    for tweet in tweets:
        if tweet.get('retweeted_status'):
            retweeted_user = tweet.get("user").get("name")
            retweeting_user = tweet.get("retweeted_status").get("user").get("name")
            tweet_id = tweet.get("retweeted_status").get("id")
            isDifferent = False
            i = -1
            for retweet in rt:
                if retweeted_user != retweet['username']:
                    isDifferent = True
                    continue
                isDifferent = False
                i += 1
                break
            
            if isDifferent:
                retweets = {'username': None, 
                'receivedRetweets': 0, 
                'tweets': {}
                }
                retweets['username'] = retweeted_user
                retweets['receivedRetweets'] = 1
                retweets['tweets'][f'tweetId: {tweet_id}'] = {'retweetedBy': [retweeting_user]}
                rt.append(retweets)
            else:
                isNotThere = True
                # Verifica si tweet_id ya está presente
                rta = rt[0]
                for rt_tweetid in rta['tweets']:
                    if tweet_id == rt_tweetid[9:]:
                        # Si tweet_id ya está presente, actualiza la lista de retweeted_by
                        rta['tweets'][rt_tweetid]['retweetedBy'].append(retweeting_user)
                        rta['receivedRetweets'] += 1
                        isNotThere = False
                        break
                
                if isNotThere:
                    # Si tweet_id no está presente, se agrega
                    rta['tweets'][f'tweetId: {tweet_id}'] = {'retweetedBy': [retweeting_user]}
                    rta['receivedRetweets'] += 1
    
    retweets = {"retweets": rt}
    
    with open('rt2.json', 'w') as f:
        json.dump(retweets, f)

def main(argv):
    input_directory = '/data'
    start_date = False
    end_date = False
    hashtags = []
    output_directory = 'app'
    
    args = argv.split()
    try:
        opts, args = getopt.getopt(args, "d:fi:ff:h:", ["grt", "jrt", "gm", "jm", "grct", "jrct"])
    except getopt.GetoptError:
        print("generador.py -d <path relativo> -fi <fecha inicial> -ff <fecha final> -h <nombre de archivo> [--grt] [--jrt] [--gm] [--jm] [--gcrt] [--jcrt]")
        sys.exit(2)
    
    flag = False
    for opt, arg in opts:
        if opt == '-d':
            #arg = correct_filepath(arg)
            input_directory = arg
        #Esto para comprobar si es -fi o -ff
        if opt == '-f' and arg == "":
            #entrará dos veces si es -ff
            if flag:
                end_date = datetime.strptime(arg, "%d-%m-%y")
                print("end_date: " + str(end_date))
            flag = True
        if opt == '-i' and flag:
            #entrará si es -fi
            start_date = datetime.strptime(arg, "%d-%m-%y")
            print("start_date: " + str(start_date))
            flag = False
        if opt == '-h':
            with open(arg, 'r') as file:
                hashtags = [line.strip() for line in file]
    
    authors = {}
    mentions = {}
    retweets = {}
    co_retweets = {}
    
    tweets = process_tweets(input_directory, start_date, end_date, hashtags)
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

main("-d data/2016/01/06/00/ -fi 06-01-16 --jrt")
