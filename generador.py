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

def process_tweet(tweet, hashtags, authors, mentions, retweets, co_retweets):
    # Procesa el tweet según tus necesidades
    print(tweet)
    # Registra las menciones y retweets
    for mention in tweet.get('mentions', []):
        mentions[mention] = mentions.get(mention, [])
        mentions[mention].append(tweet['id'])
    if 'retweet' in tweet:
        retweeted_id = tweet['retweet']
        retweets[retweeted_id] = retweets.get(retweeted_id, [])
        retweets[retweeted_id].append(tweet['author'])
        co_retweets[(tweet['author'], retweeted_id)] = co_retweets.get((tweet['author'], retweeted_id), 0) + 1
        





def main(argv):
    
    input_directory = 'app'
    start_date = False
    end_date = False
    hashtags_file = None
    output_directory = 'app'
    generate_graph_rt = False
    generate_json_rt = False
    generate_graph_mention = False
    generate_json_mention = False
    generate_graph_corretweet = False
    generate_json_corretweet = False
    
    args = argv.split()
    try:
        opts, args = getopt.getopt(args, "d:fi:ff:h:", ["grt", "jrt", "gm", "jm", "grct", "jrct"])
        print(opts)
    except getopt.GetoptError:
        print("generador.py -d <path relativo> -fi <fecha inicial> -ff <fecha final> -h <nombre de archivo> [--grt] [--jrt] [--gm] [--jm] [--gcrt] [--jcrt]")
        sys.exit(2)
    
    flag = False
    for opt, arg in opts:
        #print("opt:" + opt + " arg: " + arg + " \n")
        if opt == '-d':
            arg = correct_filepath(arg)
            input_directory = arg
        #Esto para comprobar si es -fi o -ff
        elif opt == '-f' and arg == "":
            #entrará dos veces si es -ff
            if flag:
                end_date = datetime.strptime(arg, "%d-%m-%y")
                print("end_date: " + str(end_date))
            flag = True
        elif opt == '-i' and flag:
            #entrará si es -fi
            start_date = datetime.strptime(arg, "%d-%m-%y")
            print("start_date: " + str(start_date))
            flag = False
        elif opt == '-h':
            hashtags_file = arg
        elif opt == '--grt':
            generate_graph_rt = True
        elif opt == '--jrt':
            generate_json_rt = True
        elif opt == '--gm':
            generate_graph_mention = True
        elif opt == '--jm':
            generate_json_mention = True
        elif opt == '--gcrt':
            generate_graph_corretweet = True
        elif opt == '--jcrt':
            generate_json_corretweet = True
    
    hashtags = []
    if hashtags_file:
        with open(hashtags_file, 'r') as file:
            hashtags = [line.strip() for line in file]

    authors = {}
    mentions = {}
    retweets = {}
    co_retweets = {}


    for root, dirs, files in os.walk(input_directory):
        for subdirs in dirs:
            for root, sub_dirs, subfiles  in os.walk(os.path.join(root, subdirs)):
                for file in subfiles:
                    if file.endswith('.bz2'):
                        with bz2.BZ2File(os.path.join(root, file), 'rb') as f:
                            for tweet in f:
                                tweet = tweet.decode('utf-8')
                                tweet = json.loads(tweet)
                                if(start_date or end_date) and not tweet.get('created_at') == None:
                                    tweet_date = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
                                    if(start_date):
                                        if (start_date <= tweet_date):
                                            if not hashtags or any(hashtag.lower() in tweet['text'].lower() for hashtag in hashtags):
                                                process_tweet(tweet, hashtags, authors, mentions, retweets, co_retweets)
                                    if(end_date):
                                        if (tweet_date <= end_date):
                                            print('End_tweet_date: ' + str(tweet_date))
                                            if not hashtags or any(hashtag.lower() in tweet['text'].lower() for hashtag in hashtags):
                                                process_tweet(tweet, hashtags, authors, mentions, retweets, co_retweets)



main("-d data/2016/01/06/ -fi 06-01-16 --grt")