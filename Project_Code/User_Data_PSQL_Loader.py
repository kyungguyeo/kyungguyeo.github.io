import psycopg2
import psycopg2.extras
import json
import os
import datetime
from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('-t', '--type', help='(sdoh/disease); to give the script an idea of which set of tables to write to', required=True)
parser.add_argument('-key', '--key', help='either the disease population or the sdoh factor.', required=True)

def processJSONs():
    """
    Takes in every JSON named users_*, user_profiles_*, and recent_tweets_* and parses them into objects that
    can be used by the loadData function
    """
    try:
        twitter_profiles = []
        users = []
        recent_tweets = []
        count = 0
        for file in os.listdir(os.path.join(os.getcwd(), 'user_profiles')):
            print file
            if file.startswith('user_profiles'):
                json_data = open(os.path.join(os.getcwd(), 'user_profiles', file)).read()
                twitter_profiles += json.loads(json_data)
        for file in os.listdir(os.path.join(os.getcwd(), 'users')):
            print file
            if file.startswith('users_'):
                json_data = open(os.path.join(os.getcwd(), 'users', file)).read()
                users += json.loads(json_data)
        for file in os.listdir(os.path.join(os.getcwd(), 'recent_tweets')):
            if file.startswith('recent_tweets_'):
                print file
                json_data = open(os.path.join(os.getcwd(), 'recent_tweets', file)).read()
                recent_tweets += json.loads(json_data)
                os.rename(os.path.join(os.getcwd(), 'recent_tweets', file), os.path.join("/Users/johnnyyeo/Desktop/recent_tweets/", file))
                count += 1
                if count == 10:
                    break
    except Exception as e:
        print e
    return twitter_profiles, recent_tweets, users

def loadData(data, tablename, **kwargs):
    """
    Loads in data. Follow different schemas, depending on what table the data is being loaded onto
    """
    conn = psycopg2.connect(user='postgres', password='MIDSw210SDOH', host='35.199.151.123',port='5432')
    cur = conn.cursor()

    if tablename == 'SDOH_Model_User_Tweet_History':
        insert_query = 'INSERT INTO SDOH_Model_User_Tweet_History (handle, tweet_text, tweet_id, tweet_datetime) VALUES %s'
        rows = []
        pkeys = []
        cur.execute('SELECT tweet_id FROM %s' % tablename)
        current_pkeys = cur.fetchall()
        current_pkeys_list = [k[0] for k in current_pkeys]
        print len(data)
        for recent_tweet in data:
            if (str(recent_tweet['id']) in current_pkeys_list) or (str(recent_tweet['id']) in pkeys):
                pass
            else:
                handle = recent_tweet['user']['screen_name']
                tweet_text = recent_tweet['text']
                tweet_id = recent_tweet['id']
                tweet_timestamp = recent_tweet['created_at']
                rows.append((handle, tweet_text, tweet_id, tweet_timestamp))
                pkeys.append(tweet_id)
        try:
            print '%s rows to insert...chunking into 100s...' % len(rows)
            i = 0
            while i < len(rows):
                if i + 10 > len(rows):
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:len(rows)])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, len(rows))
                    print [r[2] for r in rows[i:len(rows)]]
                else:
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:i+10])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, i+9)
                    print [r[2] for r in rows[i:i+10]]
                    i += 10
            cur.close()
            print 'data loaded to %s.' % tablename
        except Exception as e:
            print e
    elif tablename == 'SDOH_Model_User':
        insert_query = 'INSERT INTO SDOH_Model_User (handle, marker_tweet, marker_tweet_id, marker_tweet_datetime, search_phrase, sdoh_model, label) VALUES %s'
        rows = []
        pkeys = []
        cur.execute('SELECT marker_tweet_id FROM %s;' % tablename)
        current_pkeys = cur.fetchall()
        current_pkeys_list = [k[0] for k in current_pkeys]
        for user_info in data:
            if (str(user_info['marker_tweet_id']) in current_pkeys_list) or (user_info['marker_tweet_id'] in pkeys):
                pass

            else:
                handle = user_info['username']
                marker_tweet = user_info['marker_tweet']
                marker_tweet_id = user_info['marker_tweet_id']
                if user_info['marker_tweet_date']:
                    marker_tweet_datetime = datetime.datetime.strptime(user_info['marker_tweet_date'], "%H:%M %p - %d %b %Y")
                else:
                    marker_tweet_datetime = None
                label = user_info['label']
                search_phrase = user_info['key_phrase']
                sdoh_model = kwargs['sdoh']
                rows.append((handle, marker_tweet, marker_tweet_id, marker_tweet_datetime, search_phrase, sdoh_model, label))
                pkeys.append(marker_tweet_id)
        try:
            print '%s rows to insert...chunking into 100s...' % len(rows)
            i = 0
            while i < len(rows):
                if i + 100 > len(rows):
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:len(rows)])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, len(rows))
                else:
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:i+100])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, i+99)
                    i += 100
            cur.close()
            print 'data loaded to %s.' % tablename
        except Exception as e:
            print e
    elif tablename == 'SDOH_Model_User_Profile_Detail':
        insert_query = 'INSERT INTO SDOH_Model_User_Profile_Detail (handle, latitude, longitude, gender, follower_count, favorites_count, friends_count, bot_likelihood) VALUES %s'
        rows = []
        pkeys = []
        cur.execute('SELECT handle FROM %s' % tablename)
        current_pkeys = cur.fetchall()
        current_pkeys_list = [k[0] for k in current_pkeys]
        for twitter_profile in data:
            if twitter_profile['screen_name'] in current_pkeys_list or twitter_profile['screen_name'] in pkeys:
                pass
            else:
                handle = twitter_profile['screen_name']
                latitude = twitter_profile['latitude']
                longitude = twitter_profile['longitude']
                gender = twitter_profile['gender']
                follower_count = twitter_profile['followers_count']
                favorites_count = twitter_profile['favourites_count']
                friends_count = twitter_profile['friends_count']
                bot_likelihood = twitter_profile['bot_likelihood']
                rows.append((handle, latitude, longitude, gender, follower_count, favorites_count, friends_count, bot_likelihood))
                pkeys.append(handle)
        try:
            print '%s rows to insert...chunking into 100s...' % len(rows)
            i = 0
            while i < len(rows):
                if i + 100 > len(rows):
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:len(rows)])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, len(rows))
                else:
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:i+100])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, i+99)
                    i += 100
            cur.close()
            print 'data loaded to %s.' % tablename
        except Exception as e:
            print e
    elif tablename == 'Disease_Subject_User':
        insert_query = 'INSERT INTO Disease_Subject_User (handle, marker_tweet, marker_tweet_id, search_phrase, disease_population, marker_tweet_datetime) VALUES %s'
        rows = []
        pkeys = []
        cur.execute('SELECT marker_tweet_id FROM %s' % tablename)
        current_pkeys = cur.fetchall()
        current_pkeys_list = [k[0] for k in current_pkeys]
        for user_info in data:
            if (str(user_info['marker_tweet_id']) in current_pkeys_list) or (user_info['marker_tweet_id'] in pkeys):
                pass
            else:
                handle = user_info['username']
                marker_tweet = user_info['marker_tweet']
                marker_tweet_id = user_info['marker_tweet_id']
                search_phrase = user_info['key_phrase']
                disease_population = kwargs['disease_population']
                if user_info['marker_tweet_date']:
                    marker_tweet_datetime = datetime.datetime.strptime(user_info['marker_tweet_date'], "%H:%M %p - %d %b %Y")
                else:
                    marker_tweet_datetime = None
                rows.append((handle, marker_tweet, marker_tweet_id, search_phrase, disease_population, marker_tweet_datetime))
                pkeys.append(marker_tweet_id)
        try:
            print '%s rows to insert...chunking into 100s...' % len(rows)
            i = 0
            while i < len(rows):
                if i + 100 > len(rows):
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:len(rows)])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, len(rows))
                else:
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:i+100])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, i+99)
                    i += 100
            cur.close()
            print 'data loaded to %s.' % tablename
        except Exception as e:
            print e
    elif tablename == 'Disease_Subject_User_Profile_Detail':
        insert_query = 'INSERT INTO Disease_Subject_User_Profile_Detail (handle, latitude, longitude, gender, follower_count, favorites_count, friends_count, bot_likelihood) VALUES %s'
        rows = []
        pkeys = []
        cur.execute('SELECT handle FROM %s' % tablename)
        current_pkeys = cur.fetchall()
        current_pkeys_list = [k[0] for k in current_pkeys]
        for twitter_profile in data:
            if twitter_profile['screen_name'] in current_pkeys_list or twitter_profile['screen_name'] in pkeys:
                pass
            else:
                handle = twitter_profile['screen_name']
                latitude = twitter_profile['latitude']
                longitude = twitter_profile['longitude']
                gender = twitter_profile['gender']
                follower_count = twitter_profile['followers_count']
                favorites_count = twitter_profile['favourites_count']
                friends_count = twitter_profile['friends_count']
                bot_likelihood = twitter_profile['bot_likelihood']
                rows.append((handle, latitude, longitude, gender, follower_count, favorites_count, friends_count, bot_likelihood))
                pkeys.append(handle)
        try:
            print '%s rows to insert...chunking into 100s...' % len(rows)
            i = 0
            while i < len(rows):
                if i + 100 > len(rows):
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:len(rows)])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, len(rows))
                else:
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:i+100])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, i+99)
                    i += 100
            cur.close()
            print 'data loaded to %s.' % tablename
        except Exception as e:
            print e
    elif tablename == 'Disease_Subject_User_Tweet_History':
        insert_query = 'INSERT INTO Disease_Subject_User_Tweet_History (handle, tweet_text, tweet_id, tweet_datetime) VALUES %s'
        rows = []
        pkeys = []
        cur.execute('SELECT tweet_id FROM %s' % tablename)
        current_pkeys = cur.fetchall()
        current_pkeys_list = [k[0] for k in current_pkeys]
        for recent_tweet in data:
            if (str(recent_tweet['id']) in current_pkeys_list) or (str(recent_tweet['id']) in pkeys):
                pass
            else:
                handle = recent_tweet['user']['screen_name']
                tweet_text = recent_tweet['text']
                tweet_id = recent_tweet['id']
                tweet_timestamp = recent_tweet['created_at']
                rows.append((handle, tweet_text, tweet_id, tweet_timestamp))
                pkeys.append(tweet_id)
        try:
            print '%s rows to insert...chunking into 100s...' % len(rows)
            i = 0
            while i < len(rows):
                if i + 100 > len(rows):
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:len(rows)])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, len(rows))
                else:
                    psycopg2.extras.execute_values(cur, insert_query, rows[i:i+100])
                    conn.commit()
                    print 'inserted row %s to %s.' % (i, i+99)
                    i += 100
            cur.close()
            print 'data loaded to %s.' % tablename
        except Exception as e:
            print e

if __name__ == "__main__":

    args = parser.parse_args()

    twitter_profiles, recent_tweets, users = processJSONs()

    if args.type == 'sdoh':
        loadData(users, 'SDOH_Model_User', sdoh=args.key)
        loadData(twitter_profiles, 'SDOH_Model_User_Profile_Detail')
        loadData(recent_tweets, 'SDOH_Model_User_Tweet_History')
    elif args.type == 'disease':
        loadData(users, 'Disease_Subject_User', disease_population=args.key)
        loadData(twitter_profiles, 'Disease_Subject_User_Profile_Detail')
        loadData(recent_tweets, 'Disease_Subject_User_Tweet_History')
