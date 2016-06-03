#import relevant libraries and set up variables' values
import twython
import configparser
import sqlite3
import os
import platform
import re
import requests
import sys
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import time
from collections import Counter
from tqdm import tqdm
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderQuotaExceeded

MaxTweetsAPI = 3200
StatusesCount = 200
WordCloudTweetNo = 40
IncludeRts = 1
ExcludeReplies = 0
SkipStatus = 1
ScreenName = 'Enter your screen name'
SentimentURL = 'http://text-processing.com/api/sentiment/'
config = configparser.ConfigParser()
config.read('params.cfg')
AppKey = config.get('TwitterAuth','AppKey')
AppSecret = config.get('TwitterAuth','AppSecret')
reload(sys)
sys.setdefaultencoding('utf-8')

#Set up database objects' schema
if platform.system() == 'Windows':
    db_location = os.path.normpath('Final_Project_Twitter_Scraping.db') # e.g. 'C:/Twitter_Scraping_Project/twitterDB.db'
else:
    db_location = r'twitterDB.db' # e.g. '/home/username/Twitter_Scraping_Project/twitterDB.db'

objectsCreate = {'UserTimeline':
                 'CREATE TABLE IF NOT EXISTS UserTimeline ('
                 'user_id int, '
                 'user_name text, '
                 'screen_name  text, '
                 'user_description text, '
                 'user_location text, '
                 'user_url text, '
                 'user_created_datetime text,'
                 'user_language text ,'
                 'user_timezone text, '
                 'user_utc_offset real,'
                 'user_friends_count real,'
                 'user_followers_count real,'
                 'user_statuses_count real,'
                 'tweet_id int,'
                 'tweet_id_str text,'
                 'tweet_text text,'
                 'tweet_created_timestamp text,'
                 'tweet_probability_sentiment_positive real,'
                 'tweet_probability_sentiment_neutral real,'
                 'tweet_probability_sentiment_negative real,'
                 'tweet_sentiment text, '
                 'PRIMARY KEY(tweet_id, user_id))',

                 'FollowersGeoData':
                 'CREATE TABLE IF NOT EXISTS FollowersGeoData ('
                 'follower_id int,'
                 'follower_name text,'
                 'follower_location text,'
                 'location_latitude real,'
                 'location_longitude real,'
                 'PRIMARY KEY (follower_id))',

                 'WordsCount':
                 'CREATE TABLE IF NOT EXISTS WordsCount ('
                 'word text,'
                 'frequency int)'}

#create database file and schema using the scripts above
db_is_new = not os.path.exists(db_location)
with sqlite3.connect(db_location) as conn:
    conn.text_factory = str
    if db_is_new:
        print("Creating database schema on " + db_location + " database...\n")
        for t in objectsCreate.items():
            try:
                conn.executescript(t[1])
            except sqlite3.OperationalError as e:
                print (e)
                conn.rollback()
                sys.exit(1)
            else:
                conn.commit()
    else:
        print('Database already exists, bailing out...')

UserTimelineIDs = []
cur = 'SELECT DISTINCT tweet_ID from UserTimeline'
data = conn.execute(cur).fetchall()
for u in data:
    UserTimelineIDs.extend(u)

UserFollowerIDs = []
cur = 'SELECT DISTINCT follower_id from FollowersGeoData'
data = conn.execute(cur).fetchall()
for f in data:
    UserFollowerIDs.extend(f)

#check Twitter API calls limit and pause execution to reset the limit if required
def checkRateLimit(limittypecheck):
    appstatus = {'remaining':1}
    while True:
        if appstatus['remaining'] > 0:
            twitter = twython.Twython(AppKey, AppSecret, oauth_version=2)
            ACCESS_TOKEN = twitter.obtain_access_token()
            twitter = twython.Twython(AppKey, access_token=ACCESS_TOKEN)
            status = twitter.get_application_rate_limit_status(resources = ['statuses', 'application', 'followers'])
            appstatus = status['resources']['application']['/application/rate_limit_status']
            if limittypecheck=='usertimeline':
                usertimelinestatus = status['resources']['statuses']['/statuses/user_timeline']
                if usertimelinestatus['remaining'] == 0:
                    wait = max(usertimelinestatus['reset'] - time.time(), 0) + 1  # addding 1 second pad
                    time.sleep(wait)
                else:
                    return
            if limittypecheck=='followers':
                userfollowersstatus = status['resources']['followers']['/followers/list']
                if userfollowersstatus['remaining'] == 0:
                    wait = max(userfollowersstatus['reset'] - time.time(), 0) + 1  # addding 1 second pad
                    time.sleep(wait)
                else:
                    return
        else :
            wait = max(appstatus['reset'] - time.time(), 0) + 1
            time.sleep(wait)


#grab user timeline twitter feed for the profile selected and store them in a table
def getUserTimelineFeeds(StatusesCount, MaxTweetsAPI, ScreenName, IncludeRts, ExcludeReplies, AppKey, AppSecret):
    #Pass Twitter API and database credentials/config parameters
    twitter = twython.Twython(AppKey, AppSecret, oauth_version=2)
    try:
        ACCESS_TOKEN = twitter.obtain_access_token()
    except twython.TwythonAuthError as e:
        print (e)
        sys.exit(1)
    else:
#        try:
        twitter = twython.Twython(AppKey, access_token=ACCESS_TOKEN)
        print('Acquiring tweeter feed for user "{0}"...'.format(ScreenName))
        params = {'count': StatusesCount, 'screen_name': ScreenName, 'include_rts': IncludeRts,'exclude_replies': ExcludeReplies}
        AllTweets = []
        checkRateLimit(limittypecheck='usertimeline')
        NewTweets = twitter.get_user_timeline(**params)
        if NewTweets is None:
            print('No user timeline tweets found for "{0}" account, exiting now...'.format(ScreenName))
            sys.exit()
        else:
            ProfileTotalTweets = [tweet['user']['statuses_count'] for tweet in NewTweets][0]
            if ProfileTotalTweets > MaxTweetsAPI:
                TweetsToProcess = MaxTweetsAPI
            else:
                TweetsToProcess = ProfileTotalTweets
            oldest = NewTweets[0]['id']
            progressbar = tqdm(total=TweetsToProcess, leave=1)
            while len(NewTweets) > 0:
                checkRateLimit(limittypecheck='usertimeline')
                NewTweets = twitter.get_user_timeline(max_id=oldest, **params)
                AllTweets.extend(NewTweets)
                oldest = AllTweets[-1]['id'] - 1
                if len(NewTweets)!=0:
                    progressbar.update(len(NewTweets))
            progressbar.close()

            AllTweets = [tweet for tweet in AllTweets if tweet['id'] not in UserTimelineIDs]
            for tweet in AllTweets:
                conn.execute("INSERT OR IGNORE INTO UserTimeline "
                 "(user_id,"
                 "user_name,"
                 "screen_name,"
                 "user_description,"
                 "user_location,"
                 "user_url,"
                 "user_created_datetime,"
                 "user_language,"
                 "user_timezone,"
                 "user_utc_offset,"
                 "user_friends_count,"
                 "user_followers_count,"
                 "user_statuses_count,"
                 "tweet_id,"
                 "tweet_id_str,"
                 "tweet_text,"
                 "tweet_created_timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(
                tweet['user']['id'],
                tweet['user']['name'],
                tweet['user']['screen_name'],
                tweet['user']['description'],
                tweet['user']['location'],
                tweet['user']['url'],
                time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
                tweet['user']['lang'],
                tweet['user']['time_zone'],
                tweet['user']['utc_offset'],
                tweet['user']['friends_count'],
                tweet['user']['followers_count'],
                tweet['user']['statuses_count'],
                tweet['id'],
                tweet['id_str'],
                str(tweet['text'].replace("\n","")),
                time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))))
            conn.commit()
#        except Exception as e:
#            print(e)

#assign sentiment to individual tweets
def getSentiment(SentimentURL):
        cur = ''' SELECT tweet_id, tweet_text, count(*) as NoSentimentRecCount
                  FROM UserTimeline
                  WHERE tweet_sentiment IS NULL
                  GROUP BY tweet_id, tweet_text '''
        data = conn.execute(cur).fetchone()
        if data is None:
            print('Sentiment already assigned to relevant records or table is empty, bailing out...')
        else:
            print('Assigning sentiment to selected tweets...')
            data = conn.execute(cur)
            payload = {'text':'tweet'}
            for t in tqdm(data.fetchall(),leave=1):
                id = t[0]
                payload['text'] = t[1]
                #concatnate if tweet is on multiple lines
                payload['text'] = str(payload['text'].replace("\n", ""))
                #remove http:// URL shortening links
                payload['text'] = re.sub(r'http://[\w.]+/+[\w.]+', "", payload['text'], re.IGNORECASE)
                #remove https:// URL shortening links
                payload['text'] = re.sub(r'https://[\w.]+/+[\w.]+', "", payload['text'], re.IGNORECASE)
                #remove certain characters
                payload['text'] = re.sub('[@#\[\]\'"$.;{}~`&amp;lt;&amp;gt;:%&amp;amp;^*()-?_!,+=]', "", payload['text'])
                #print(payload['text'])
                try:
                    post=requests.post(SentimentURL, data=payload)
                    response = post.json()
                    conn.execute("UPDATE UserTimeline "
                                    "SET tweet_probability_sentiment_positive = ?, "
                                    "tweet_probability_sentiment_neutral = ?, "
                                    "tweet_probability_sentiment_negative = ?, "
                                    "tweet_sentiment = ? WHERE tweet_id = ?",
                                    (response['probability']['neg'],
                                    response['probability']['neutral'],
                                    response['probability']['pos'],
                                    response['label'], id))
                    conn.commit()
                except Exception as e:
                    print (e)
                    
                    
#get most commonly occurring (40) words in all stored tweets
def getWordCounts(WordCloudTweetNo):
    print('Fetching the most commonly used {0} words in the "{1}" feed...'.format(WordCloudTweetNo, ScreenName))
    cur = "DELETE FROM WordsCount;"
    conn.execute(cur)
    conn.commit()
    cur = 'SELECT tweet_text FROM UserTimeline'
    data = conn.execute(cur)
    StopList = stopwords.words('english')
    Lem = WordNetLemmatizer()
    AllWords = ''
    for w in tqdm(data.fetchall(),leave=1):
            try:
                #remove certain characters and strings
                CleanWordList = re.sub(r'http://[\w.]+/+[\w.]+', "", w[0], re.IGNORECASE)
                CleanWordList = re.sub(r'https://[\w.]+/+[\w.]+', "", CleanWordList, re.IGNORECASE)
                CleanWordList = re.sub(r'[@#\[\]\'"$.;{}~`<>:%&^*()-?_!,+=]', "", CleanWordList)
                #tokenize and convert to lower case
                CleanWordList = [words.lower() for words in word_tokenize(CleanWordList) if words not in StopList]
                #lemmatize words
                CleanWordList = [Lem.lemmatize(word) for word in CleanWordList]
                #join words
                CleanWordList =' '.join(CleanWordList)
                AllWords += CleanWordList
            except Exception as e:
                print (e)
                sys.exit(e)
    if AllWords is not None:
        words = [word for word in AllWords.split()]
        c = Counter(words)
        for word, count in c.most_common(WordCloudTweetNo):
            conn.execute("INSERT INTO WordsCount (word, frequency) VALUES (?,?)", (word, count))
            conn.commit()

#geocode followers where geolocation data stored as part of followers' profiles
def GetFollowersGeoData(StatusesCount, ScreenName, SkipStatus, AppKey, AppSecret):
    print('Geo-coding followers location where location variable provided in the user profile...')
    geo = GoogleV3(timeout=5)
    cur = 'SELECT follower_id, follower_location FROM FollowersGeoData WHERE location_latitude IS NULL OR location_longitude IS NULL'
    data = conn.execute(cur)
    for location in tqdm(data.fetchall(), leave=1):
        try:
            try:
                followerid = location[0]
                #print(location[1])
                geoparams = geo.geocode(location[1])
            except GeocoderQuotaExceeded as e:
                print(e)
                return
                #print(geoparams)
            else:
                if geoparams is None:
                    pass
                else:
                    latitude = geoparams.latitude
                    longitude = geoparams.longitude
                    conn.execute("UPDATE FollowersGeoData "
                                "SET location_latitude = ?,"
                                "location_longitude = ?"
                                "WHERE follower_id = ?",
                                (latitude,longitude,followerid))
                    conn.commit()
        except Exception as e:
            print("Error: geocode failed on input %s with message %s"%(location[2], e))
            continue

#run all functions
def main():
    getUserTimelineFeeds(StatusesCount, MaxTweetsAPI, ScreenName, IncludeRts, ExcludeReplies, AppKey, AppSecret)
    getSentiment(SentimentURL)
    getWordCounts(WordCloudTweetNo)
    GetFollowersGeoData(StatusesCount, ScreenName, SkipStatus, AppKey, AppSecret)
    conn.close()

if __name__ == "__main__":
    main()

