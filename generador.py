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
        for subdir in dirs:
            new_path = os.path.join(root, subdir)
            for subroot, subsdirs, subfiles in os.walk(new_path):
                for file in subfiles:
                    if file.endswith('.bz2'):
                        print(file)
                        with bz2.BZ2File(os.path.join(subroot, file), 'rb') as f:
                            for line in f:
                                line = line.decode('utf-8')
                                tweet = json.loads(line)
                                created_at = tweet.get('created_at')
                                
                                if (start_date or end_date and created_at) or len(hashtags) > 0:
                                    tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
                                    if start_date and not end_date and start_date <= tweet_date:
                                        tweets.append(tweet)
                                    if end_date and not start_date and tweet_date <= end_date:
                                        tweets.append(tweet)
                                    if end_date and start_date and (start_date <= tweet_date <= end_date):
                                        tweets.append(tweet)
                                    for hashtag in tweet['entities']['hashtags']:
                                        if hashtag['text'] in hashtags:
                                            tweets.pop()
    return tweets



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
    
    tweets = process_tweets(input_directory, start_date, end_date, hashtags)
    
    


main("-d data/2016/01/06/ -fi 06-01-16 --grt")