import tweepy
import json
import carmen
import botometer
import datetime
import time
import os
from demographer import process_tweet
from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('-f', '--file', help='name of json file to parse through', required=True)
parser.add_argument('-rt', '--recent_tweets', help='number of recent tweets to grab for each profile. defaults to 10.', required=False)
parser.add_argument('-s', '--start_at', help='username to start at.', required=False)

def processData(json_file, rts=10, start_at=None):
    """
    takes in json file of either sdoh risk training dataset or disease subjects. 

    for each user, grabs its user profile data and the recent tweets from the tweepy api.

    adds location and gender data to the user profiles.

    returns a last_n_tweets json object and a user_profile json object.
    """
    #Twitter Creds
#     twitter_app_auth = {
#         'consumer_key': '',
#         'consumer_secret': '',
#         'access_token': '',
#         'access_token_secret': ''
#     }

    # API setup
    auth = tweepy.OAuthHandler(twitter_app_auth['consumer_key'], twitter_app_auth['consumer_secret'])
    auth.set_access_token(twitter_app_auth['access_token'], twitter_app_auth['access_token_secret'])
    api = tweepy.API(auth)

    # Carmen setup
    resolver = carmen.get_resolver()
    resolver.load_locations()

    # File setup 
    file_directory = json_file
    json_data=open(file_directory).read()
    users = json.loads(json_data)

    if start_at:
        start_indx = [users.index(user) for user in users if user['username'] == start_at]
        users = users[start_indx[0]:]

    # Mashape Key for botometer
    mashape_key = 'TonZ1SlGz7mshDB8TSdsbjQebLgHp16UAtojsnSFkac2fxpBTa'

    # Filter for twitter profiles in the US - just do 20 profiles by default
    twitter_profiles = []
    all_recent_tweets = []
    usa_usernames = []
    counter = 0
    for tweet in users:
        try:
            if tweet['username'] not in usa_usernames:
                profile = api.get_user(tweet['username'], wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
                recent_tweets = api.user_timeline(tweet['username'], count=rts, max_id=int(tweet['marker_tweet_id'])-1)
                if recent_tweets:
                    recent_tweet = recent_tweets[0]
                    location = resolver.resolve_tweet(recent_tweet._json)
                else:
                    location = None
                if location:
                    if location[1].country == 'United States':
                        print 'processing %s...' % tweet['username']
                        print 'recent tweets for %s: %s' % (tweet['username'], len(recent_tweets))
                        profile._json['county'] = location[1].county
                        profile._json['latitude'] = location[1].latitude
                        profile._json['longitude'] = location[1].longitude
                        profile = add_gender(profile, recent_tweet)
                        # is it a bot?
                        bom = None
                        while not bom:
                            try:
                                print 'checking whether or not its a bot...'
                                bom = botometer.Botometer(wait_on_ratelimit=True, mashape_key=mashape_key, **twitter_app_auth)
                            except Exception as e:
                                print 'probably timeout error. Waiting 1 minute before trying again...'
                                time.sleep(60)
                        result = bom.check_account(tweet['username'])
                        profile._json['bot_likelihood'] = result['scores']['universal']
                        twitter_profiles.append(profile)
                        all_recent_tweets.append(recent_tweets)
                        usa_usernames.append(tweet['username'])
                        counter += 1
                        if counter == 100:
                            print '100 profiles hit; writing jsons before moving onto the next batch.'
                            usa_users = [x for x in users if x['username'] in usa_usernames]
                            final_twitter_profiles = [user._json for user in twitter_profiles]
                            final_recent_tweets = [status._json for recent_tweets in all_recent_tweets for status in recent_tweets]
                            print 'processed %s (%s) profiles.' % (counter, len(usa_users))
                            print '%s recent tweets. ' % len(final_recent_tweets)
                            write_to_json(final_twitter_profiles, final_recent_tweets, usa_users, json_file)
                            twitter_profiles = []
                            all_recent_tweets = []
                            usa_usernames = []
                            counter = 0
        except tweepy.TweepError as e:
            print e.message
            if 'Failed to send request:' in e.reason:
                print "Time out error caught."
                time.sleep(180)
            elif e.message == 'Not authorized.':
                pass
            elif e.message[0]['message'] == 'Rate limit exceeded':
                print 'rate limit exceeded. waiting 15 minutes...'
                time.sleep(60 * 15)
    usa_users = [x for x in users if x['username'] in usa_usernames]
    final_twitter_profiles = [user._json for user in twitter_profiles]
    final_recent_tweets = [status._json for recent_tweets in all_recent_tweets for status in recent_tweets]
    print 'processed %s (%s) profiles.' % (counter, len(usa_users))
    print '%s recent tweets. ' % len(final_recent_tweets)
    return final_twitter_profiles, final_recent_tweets, usa_users

def write_to_json(twitter_profiles, recent_tweets, users, filename):
    """
    Takes in an array of users and recent_tweets and writes them to a json file
    """
    twitter_profiles_json_str = json.dumps(twitter_profiles, indent = 4, sort_keys=True, ensure_ascii=True)
    recent_tweets_json_str = json.dumps(recent_tweets, indent = 4, sort_keys=True, ensure_ascii=True)
    users_json_str = json.dumps(users, indent = 4, sort_keys=True, ensure_ascii=True)
    # write json if needed
    with open('users/users_' + str(datetime.datetime.now()).replace('-','').replace(' ','') + "_" + args.file.split('/')[-1], 'w') as f:
        f.write(users_json_str)
    with open('recent_tweets/recent_tweets_' + str(datetime.datetime.now()).replace('-','').replace(' ','') + "_" + args.file.split('/')[-1], 'w') as f:
        f.write(recent_tweets_json_str)
    with open('user_profiles/user_profiles_' + str(datetime.datetime.now()).replace('-','').replace(' ','') + "_" + args.file.split('/')[-1], 'w') as f:
        f.write(twitter_profiles_json_str)
    print 'json files have been written.'

def add_gender(user, tweet):
    """
    Takes in a user profile tweet and adds gender to the tweet json using demographer
    """
    # Gender setup
    gender_dict = {'M': 'Male', 'F': 'Female'}

    gender_data = process_tweet(tweet._json)
    if len(gender_data[0]['gender']) == 1:
        if gender_dict[gender_data[0]['gender'][0]['value']]:
            gender = gender_dict[gender_data[0]['gender'][0]['value']]
        else:
            gender = 'Unknown'
    elif len(gender_data[0]['gender']) == 2:
        if gender_data[0]['gender'][0]['prob'] > gender_data[0]['gender'][1]['prob']:
            gender = gender_dict[gender_data[0]['gender'][0]['value']]
        elif gender_data[0]['gender'][0]['prob'] < gender_data[0]['gender'][1]['prob']:
            gender = gender_dict[gender_data[0]['gender'][1]['value']]
        else:
            gender = 'Unknown'
    else:
        gender = 'Unknown'
    user._json['gender'] = gender

    return user


if __name__ == "__main__":

    args = parser.parse_args()

    # Optional args
    if args.recent_tweets:
        rts = int(args.recent_tweets)
    else:
        rts = 10

    if args.start_at:
        start_at = args.start_at
    else:
        start_at = None

    twitter_profiles, recent_tweets, users = processData(args.file, rts, start_at)
    print 'writing all the data to json files...'
    write_to_json(twitter_profiles, recent_tweets, users, args.file)
