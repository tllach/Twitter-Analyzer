import glob
import networkx as nx 
import json 
import time
from itertools import combinations
import bz2,sys
from collections import defaultdict, OrderedDict
from datetime import datetime
import concurrent.futures
from itertools import combinations
import multiprocessing
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
numProcess = comm.Get_size()

def correct_filepath(path: str):
    if path.startswith('/') or path.startswith('\\'):
        path = path[1:]
    return path.replace('/', '\\').strip()

def is_valid_tweet(tweet, start_date, end_date, hashtags):
    created_at = tweet.get('created_at')
    if not start_date and not end_date and not hashtags:
        return True
    if not start_date and not end_date and hashtags:
        return hashtags and any(hashtag['text'].lower() in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', []))
    if created_at:
        tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
        date_condition = (start_date and tweet_date >= start_date) or (end_date and tweet_date <= end_date)
        hashtag_condition = not hashtags or any(hashtag['text'].lower() in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', []))
        return date_condition and hashtag_condition
    return False

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
    if rank == 0:
        if input_directory.endswith('.bz2'):
            tweets = []
            process_bz2_file(input_directory, start_date, end_date, hashtags, tweets)
            return tweets
        else:
            #Si se pasa un directorio
            allFiles = list(glob.iglob(f"{input_directory}/**/*.json.bz2", recursive=True))
            print(allFiles)
            print(allFiles.count)
            len_allFiles = len(allFiles)
            numFilesProcess = len_allFiles // numProcess
            remainder = len_allFiles % numProcess
            allTweetsFiles = []
            for process in range(numProcess):
                start = process * numFilesProcess + min(process, remainder)
                end = start + numFilesProcess + (1 if process < remainder else 0)
                allTweetsFiles.append(allFiles[start:end])
    else:
        allTweetsFiles = None

    #Para mandar los tweets a los procesos (incluido root)
    tweetfiles = comm.scatter(allTweetsFiles, root=0)
    
    tweets = []
    for archivo in tweetfiles:
        print(f'Process {rank}:', archivo)
        process_bz2_file(archivo, start_date, end_date, hashtags, tweets)
    
    allTweets = comm.gather(tweets, root=0)
    
    if rank == 0:
        tweets = []
        for tweet in allTweets:
            tweets.extend(tweet)
            print("tweets procesados count:", tweets.count)
        return tweets
    
    return None

def generate_minigraph_rt(tweets: list):
    G = nx.DiGraph()
    for tweet in tweets:
        try:
            tweet_rt = tweet.get('retweeted_status')
            if tweet_rt:
                retweeting_user = tweet['user']['screen_name']
                retweeted_user = tweet_rt['user']['screen_name']
                if not G.has_edge(retweeted_user, retweeting_user):
                    G.add_edge(retweeted_user, retweeting_user)
        except (KeyError, TypeError) as e:
            print(f"Error processing tweet: {e}")
    return G

def generate_graph_rt(tweets: list):
    
    subgraph = generate_minigraph_rt(tweets[rank])
    
    allSubgraphs = comm.gather(subgraph, root=0)
    
    # Combinar subgrafos
    if rank == 0:
        G = nx.DiGraph()
        for sg in allSubgraphs:
            G = nx.compose(G, sg)
        nx.write_gexf(G, 'rtp.gexf')
        return G
    return None

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

def create_retweet_minijson(tweets):
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
            else:
                retweet_data['tweets'][tweet_id]['retweetedBy'].append(retweeting_user)
            retweet_data['receivedRetweets'] += 1
    
    return retweets


def funcion(alltwts: list):
    
    subjson = create_retweet_minijson(alltwts[rank])
    
    allSubjsons = comm.gather(subjson, root=0)
    
    if rank == 0:
            combined_retweets = {}
            for partial in allSubjsons:
                for user, data in partial.items():
                    if user not in combined_retweets:
                        combined_retweets[user] = data
                    else:
                        combined_retweets[user]['receivedRetweets'] += data['receivedRetweets']
                        for tweet_id, tweet_data in data['tweets'].items():
                            if tweet_id not in combined_retweets[user]['tweets']:
                                combined_retweets[user]['tweets'][tweet_id] = tweet_data
                            else:
                                combined_retweets[user]['tweets'][tweet_id]['retweetedBy'].extend(tweet_data['retweetedBy'])

            sorted_retweets = sorted(combined_retweets.items(), key=lambda x: x[1]['receivedRetweets'], reverse=True)
            result = {"retweets": [{'username': key, **value} for key, value in sorted_retweets]}
            return result

    return None

def generate_minigraph_mention(tweets: list):
    G = nx.DiGraph()
    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            tweeting_user = tweet['user']['screen_name']
            for mention in tweet['entities']['user_mentions']:
                mentioned_user = mention['screen_name']
                if not G.has_edge(tweeting_user, mentioned_user):
                    G.add_edge(tweeting_user, mentioned_user)
    return G

def generate_graph_mention(tweets: list):
    subgraph = generate_minigraph_mention(tweets[rank])
    
    allSubgraphs = comm.gather(subgraph, root=0)
    
    # Combinar subgrafos
    if rank == 0:
        G = nx.DiGraph()
        for sg in allSubgraphs:
            G = nx.compose(G, sg)
        nx.write_gexf(G, 'menciónp.gexf')
        return G

    return None

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

    with open('menciónp.json', 'w') as f:
        json.dump(sorted_mentions, f, indent=4)

def generate_graph_corretweet(tweetsjson: list, graph):
    G = nx.DiGraph()

    for entry in tweetsjson["coretweets"]:
        author1 = entry["authors"]["u1"]
        author2 = entry["authors"]["u2"]
        weight = entry["totalCoretweets"]

        if G.has_edge(author1, author2):
            G[author1][author2]['weight'] += weight
        else:
            G.add_edge(author1, author2, weight=weight)

    
    return G

def generate_graph_corretweet(tweets:list):
    subgraph = generate_minigraph_mention(tweets[rank])
    
    allSubgraphs = comm.gather(subgraph, root=0)
    
    if rank == 0:
        G = nx.DiGraph()
        for sg in allSubgraphs:
            G = nx.compose(G, sg)
        nx.write_gexf(G, 'corrtwp.gexf')
        return G


def generate_json_coretweet(tweets: list):
    json_co = {}
    
    rtweeters = {}
    for retweet_data in tweets["retweets"]:
        retweet_users = []
        for tweet_info in retweet_data["tweets"].values():
            retweet_users.extend(tweet_info["retweetedBy"])
        
        rtweeters[retweet_data["username"]] = retweet_users
    
    user_combinations = combinations(rtweeters.keys(), 2)

    for user1, user2 in user_combinations:
        retweeters1 = set(rtweeters[user1])
        retweeters2 = set(rtweeters[user2])

        common_retweeters = retweeters1 & retweeters2
        total_core_tweets = len(common_retweeters)

        if total_core_tweets > 0:
            key = (user1, user2)
            if key in json_co or (user2, user1) in json_co:
                existing_retweeters = set(json_co.get(key, []))
                new_retweeters = list(existing_retweeters - common_retweeters) + list(common_retweeters - existing_retweeters)
                json_co[key] = {
                    "authors": {"u1": user1, "u2": user2},
                    "totalCoretweets": len(new_retweeters),
                    "retweeters": new_retweeters,
                }
            else:
                json_co[key] = {
                    "authors": {"u1": user1, "u2": user2},
                    "totalCoretweets": total_core_tweets,
                    "retweeters": list(common_retweeters),
                }
    
    dic = {"coretweets": list(json_co.values())}
    
    return dic

def generate_json_coretweet_partial(retweet_data):
    partial_result = {}
    for user1, user2 in combinations(retweet_data.keys(), 2):
        retweeters1 = set(retweet_data[user1])
        retweeters2 = set(retweet_data[user2])

        common_retweeters = retweeters1 & retweeters2
        total_core_tweets = len(common_retweeters)

        if total_core_tweets > 0:
            key = (user1, user2)
            if key in partial_result or (user2, user1) in partial_result:
                existing_retweeters = set(partial_result.get(key, []))
                new_retweeters = list(existing_retweeters - common_retweeters) + list(common_retweeters - existing_retweeters)
                partial_result[key] = {
                    "authors": {"u1": user1, "u2": user2},
                    "totalCoretweets": len(new_retweeters),
                    "retweeters": new_retweeters,
                }
            else:
                partial_result[key] = {
                    "authors": {"u1": user1, "u2": user2},
                    "totalCoretweets": total_core_tweets,
                    "retweeters": list(common_retweeters),
                }
    return partial_result

def generate_json_coretweet2(tweets: list):
    with multiprocessing.Manager() as manager:
        json_co = manager.dict()

        retweet_data_list = {}
        for retweet_data in tweets["retweets"]:
            retweet_users = []
            for tweet_info in retweet_data["tweets"].values():
                retweet_users.extend(tweet_info["retweetedBy"])
            
            retweet_data_list[retweet_data["username"]] = retweet_users

        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = list(executor.map(generate_json_coretweet_partial(retweet_data_list), [retweet_data_list]))

        for result in results:
            json_co.update(result)

    dic = {"coretweets": list(json_co.values())}
    return dic

def dividir_lista(tweets:list , numProcess: int) -> list:
    list_of_tweets = []
    len_tweets = len(tweets)
    avg = len_tweets // numProcess
    remainder = len_tweets % numProcess
    
    for process in range(numProcess):
        start = process * avg + min(process, remainder)
        end = start + avg + (1 if process < remainder else 0)
        list_of_tweets.append(tweets[start:end])
    
    return list_of_tweets

def main(argv):
    ti = time.time()
    input_directory = 'input/2016/01/01/01/01.json.bz2'
    start_date = False
    end_date = False
    hashtags = []
    retweets = {}
    json_coretweet = {}
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
                opts.append((argumento, ''))
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
    
    if rank == 0:
        tweets = process_tweets(input_directory, start_date, end_date, hashtags)
        list_of_tweets = dividir_lista(tweets, numProcess)
        for opt, arg in opts:
            if opt == '--grt' or opt == '-grt':
                G = generate_graph_rt(list_of_tweets)
                nx.write_gexf(G, 'rtp.gexf')
            if opt == '--jrt' or opt == '-jrt':
                if not retweets:
                    retweets = funcion(list_of_tweets)
                with open('rtp.json', 'w') as f:
                    json.dump(retweets, f, indent=4)
            
            if opt == '--gm' or opt == '-gm':
                generate_graph_mention(list_of_tweets)
            
            if opt == '--jm' or opt == '-jm':
                generate_json_mention(tweets)
            
            if opt == '--gcrt' or opt == '-gcrt':
                if not json_coretweet:
                    if not retweets:
                        retweets = create_retweet_json(tweets)
                    json_coretweet = generate_json_coretweet2(retweets)
                
                json_coretweet = json
                generate_graph_corretweet(json_coretweet)
            
            if opt == '--jcrt' or opt == '-jcrt':
                if not json_coretweet: 
                    if not retweets:
                        retweets = create_retweet_json(tweets)
                    json_coretweet = generate_json_coretweet(retweets)
                
                with open('corrtwp.json', 'w') as f:
                    json.dump(json_coretweet, f, indent=4)
    
    tf = time.time()
    
    if rank == 0:
        print(tf - ti)

if __name__ == "__main__":
    if rank == 0:
        main(sys.argv[1:])