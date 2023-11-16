import os
import networkx as nx 
import json 
import time
import shutil,bz2,getopt,sys
from collections import defaultdict, OrderedDict
import bz2
from datetime import datetime
import graphlib


def correct_filepath(path: str):
    if path.startswith('/') or path.startswith('\\'):
        path = path[1:]
    return path.replace('/', '\\').strip()

def is_valid_tweet(tweet, start_date, end_date, hashtags):
    created_at = tweet.get('created_at')
    if not start_date and not end_date and not hashtags:
        return True
    if not start_date and not end_date and hashtags:
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

def process_tweets(input_directory: str, start_date, end_date, hashtags: list) -> list:
    tweets = []
    if input_directory.endswith('.bz2'):
        process_bz2_file(input_directory, start_date, end_date, hashtags, tweets)
    else:
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
    nx.write_gexf(G, 'rt.gexf')

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
    
    return result

def generate_graph_mention(tweets: list):
    G = nx.DiGraph()
    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            tweeting_user = tweet['user']['screen_name']
            for mention in tweet['entities']['user_mentions']:
                mentioned_user = mention['screen_name']
                G.add_edge(tweeting_user, mentioned_user)
    nx.write_gexf(G, 'mención.gexf')
    return G

def generate_json_mention(tweets: list):
    mentions_dict = defaultdict(lambda: OrderedDict([('username', ''), ('receivedMentions', 0), ('mentions', [])]))

    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities'] and not tweet.get('retweeted_status'):
            mentioning_user = tweet['user']['screen_name']
            tweet_id = tweet.get('id')

            for user_mention in tweet['entities']['user_mentions']:
                mentioned_user = user_mention['screen_name']
                mention_data = {'mentionBy': mentioning_user, 'tweets': [tweet_id]}
                flag = True
                mentions_dict[mentioned_user]['receivedMentions'] += 1
                for mention_m in mentions_dict[mentioned_user]['mentions']:
                    if mention_m['mentionBy'] == mentioning_user:
                        mention_m['tweets'].append(tweet_id)
                        flag = False
                        break
                if flag: mentions_dict[mentioned_user]['mentions'].append(mention_data)
                mentions_dict[mentioned_user]['username'] = mentioned_user

    # Organizar la lista de menciones por 'receivedMentions' de mayor a menor
    sorted_mentions = {'mentions':sorted(mentions_dict.values(), key=lambda x: x['receivedMentions'], reverse=True)}

    with open('mención.json', 'w') as f:
        json.dump(sorted_mentions, f, indent=4)

def generate_graph_corretweet(tweets: list):
    pass

def pasa():
    new_json = []
    retweet_data = []
    author1 = retweet_data["username"]
    retweeters = set()

    for tweet_id, tweet_info in retweet_data["tweets"].items():
        retweeters.update(tweet_info["retweetedBy"])

        coretweet_entry = {
            "authors": {"u1": author1, "u2": None},
            "totalCoretweets": 0,
            "retweeters": list(retweeters)
        }

        # Verificar si hay una relación entre usuarios de retweets y actualizar la entrada
        for other_retweet_data in tweets["retweets"]:
            if other_retweet_data["username"] != author1:
                common_retweeters = set(retweeters).intersection(
                    user for tweet in other_retweet_data["tweets"].values() for user in tweet["retweetedBy"]
                )

            if common_retweeters:
                coretweet_entry["authors"]["u2"] = other_retweet_data["username"]
                coretweet_entry["totalCoretweets"] += 1


        new_json["coretweets"].append(coretweet_entry)

def generate_json_corretweet(tweets: list):
    json_co = []
    
    rtweeters = {}
    for retweet_data in tweets["retweets"]:
        retweet_users = []
        for tweet_info in retweet_data["tweets"].values():
            retweet_users.extend(tweet_info["retweetedBy"])
        
        rtweeters[retweet_data["username"]] = retweet_users
    
    i = 0
    for username, lst_user in rtweeters.items():
        j = 0
        for username2 , lst_user2 in rtweeters.items():
            if i != j:
                # Encuentra la intersección de las dos listas
                elementos_comunes = set(lst_user) & set(lst_user2)
                lista_resultante = list(elementos_comunes)
                if elementos_comunes:
                    flag = True
                    for cort in json_co:
                        if (cort['authors']['u1'] == username or cort['authors']['u1'] == username2) and (cort['authors']['u2'] == username or cort['authors']['u2'] == username2):
                            set1 = set(cort['retweeters'])
                            set2 = set(lista_resultante)
                            diferencia_lista1 = list(set1 - set2)
                            diferencia_lista2 = list(set2 - set1)
                            interseccion = list(set1 & set2)

                            # Agregar los elementos diferentes a una nueva lista
                            nueva_lista = diferencia_lista1 + interseccion + diferencia_lista2
                            cort['totalCoretweets'] = len(nueva_lista)
                            cort['retweeters'] = nueva_lista
                            flag = False
                            break
                    if flag:
                        dic_co = {
                            "authors":{
                                'u1': username,
                                'u2': username2,
                            },
                            'totalCoretweets': len(lista_resultante),
                            'retweeters': lista_resultante
                        }
                        json_co.append(dic_co)
            j += 1
        i += 1
    
    dic = {"coretweets": json_co}
    
    
    with open('corrtw.json', 'w') as f:
        json.dump(dic, f, indent=4)

def main(argv):
    ti = time.time()
    input_directory = '/data'
    start_date = False
    end_date = False
    hashtags = []
    retweets = {}
    opts = []
    
    i = 0
    while i < len(argv):
        argumento = argv[i]
        valor = argv[i + 1] if i + 1 < len(argv) else ''
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
    
    tweets = process_tweets(input_directory, start_date, end_date, hashtags)
    
    for opt, arg in opts:
        if opt == '--grt':
            generate_graph_rt(tweets)
        if opt == '--jrt':
            retweets = create_retweet_json(tweets)
            with open('rt.json', 'w') as f:
                json.dump(retweets, f, indent=4)
        if opt == '--gm':
            generate_graph_mention(tweets)
        if opt == '--jm':
            generate_json_mention(tweets)
        if opt == '--gcrt':
            generate_graph_corretweet(tweets)
        if opt == '--jcrt':
            if not retweets:
                retweets = create_retweet_json(tweets)
            generate_json_corretweet(retweets)
    tf = time.time()
    print(tf - ti)

if __name__ == "__main__":
    main(sys.argv[1:])