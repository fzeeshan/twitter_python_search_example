from TwitterSearch import *
import time
from textblob import TextBlob
from gender_detector import GenderDetector
from geopy.geocoders import Nominatim
import datetime
import json
import sys
import requests
from gemfire import *

REGION = "encompass"
BASE_URI = "http://vps73251.vps.ovh.ca:9090/gemfire-api/v1"
headers = {'content-type': 'application/json'}



var = 1
while var == 1 :  # This constructs an infinite loop, there are other ways, trying very basic way to keep it going.
    detector = GenderDetector() # It can also be ar, uk, uy. works most of the times, with all the names
    try:
        file_name = time.strftime("%Y-%m-%d-%H%M%S")
        file_name = file_name+".json"
        fo = open(file_name, "w+")
        print "Creating a local file to save tweets: ", fo.name
        tso = TwitterSearchOrder()
        #tso.set_keywords(['starbucks', 'red bull', 'dunkin donut', '#starbucks'], or_operator = True)
        tso.set_keywords(['Not your Father\'s Root Beer','not your father\'s ginger ale','Henry\'s hard orange soda','Henrys hard ginger ale'], or_operator = True)
        tso.set_language('en')
        ts = TwitterSearch(
            consumer_key = 'consumer_key',
            consumer_secret = 'consumer_secret',
            access_token = 'access_token',
            access_token_secret = 'access_token_secret'
         )
        sleep_for = 60 # sleep for 60 seconds, using twitter recommended way of doing this
        last_amount_of_queries = 0 # used to detect when new queries are done
        def resource_uri(res=None, region=REGION):
            if res:
                return "%s/%s/%s" % (BASE_URI, region, res)
            return "%s/%s" % (BASE_URI, region)
        
        for tweet in ts.search_tweets_iterable(tso):
            #initialize variables needed later
            user_lat = None
            user_lon = None
            hashtags = []
            sentiment = None
            user_gender = []
            keys = []
            parse_tweet = TextBlob(tweet['text'])
            parse_tweet.sentiment
            polarity = parse_tweet.sentiment.polarity
            subjectivity = parse_tweet.sentiment.subjectivity
            tweet_id = tweet['id_str']
            tweet_text = tweet['text'].replace("\n","")
            tweet_text = tweet['text'].replace("\t","")
            tweet_text = tweet['text'].replace("'","`")
            tweet_text = tweet['text'].replace("\"","``")
            tweet_lang = tweet['lang']
            tweet_create_date = tweet['created_at']
            tweet_source = tweet['source']
            #polarity sentiment can be changed, using negative < 0, positive >0 neutral = 0
            if (polarity < 0):
                #print "negative"
                sentiment = "negative"
            if (polarity == 0):
                #print "neutral"
                sentiment = "neutral"
            if (polarity > 0):
                #print "positive"
                sentiment = "positive"

            #guess gender
            get_name = tweet['user']['name']
            try:        
                index = get_name.find(' ')
                name_for_gender_check = get_name[0:index]
                user_gender = detector.guess(name_for_gender_check)
            except:
                user_gender = 'unknown'
            #build variables
            user_location = tweet['user']['location']
            user_location = user_location.replace("'","`")
            user_location = user_location.replace("\n","")
            user_location = user_location.replace("\t","")
            
            user_real_name = tweet['user']['name']
            user_screen_name = tweet['user']['screen_name']
            user_description = tweet['user']['description']
            user_description = user_description.replace("'","`")
            user_description = user_description.replace("\n","")
            user_description = user_description.replace("\t","")
            user_create_date = tweet['created_at']
            user_lang = tweet['user']['lang']
            tweet_source = tweet_source.replace("\"","``")
            ##itterate through hashtags
            for hashtag in tweet['entities']['hashtags']:
                hashtags.append(hashtag['text'])
            tweet_hashtags = str(hashtags).encode('utf-8')
            tweet_hashtags = tweet_hashtags.replace("u","")
            if tweet['coordinates'] is not None:
                user_lat = tweet['coordinates']['coordinates'][0]
                user_lon = tweet['coordinates']['coordinates'][1]
                #debug: print "found lat lon in the tweet",user_lat,":",user_lon
            else:
                try:
                    if user_lat is None: 
                        get_user_location = tweet['user']['location']
                        geolocator = Nominatim()
                        location = geolocator.geocode(get_user_location)
                        user_lat = location.latitude
                        user_lon = location.longitude
                        #print "Grabbed Coordinates using geolocator"
                except:
                    #print "error catch"
                    pass

            keys.append(tweet_id)
            input_data = []
            input_data = {
            'type' : 'tweet',
            'tweet_id' : tweet_id,
            'tweet_text' : tweet_text,
            'sentiment' : sentiment,
            'polarity' : polarity,
            'subjectivity' : subjectivity,
            'tweet_create_date' : tweet_create_date,
            'tweet_lang' : tweet_lang,
            'tweet_source' : tweet_source,
            'user_lat' : user_lat,
            'user_lon' : user_lon,
            'user_gender' : user_gender,
            'user_location' : user_location,
            'user_real_name' : user_real_name,
            'user_screen_name' : user_screen_name,
            'user_create_date' : user_create_date,
            'hashtags' : hashtags,
            'time_stamp' : format(datetime.datetime.now())
            }
            json_str = json.dumps(input_data)
            print json_str
            #load into Gemfire/Apache Geode Database
            r = requests.post(resource_uri(), data=json_str,
                      params={'key': tweet_id},
              headers=headers)
        
            #save the tweets to a file as a backup, it will contain duplicates
            fo.write(json_str)
            #r.raise_for_status()
            current_amount_of_queries = ts.get_statistics()[0]
            if not last_amount_of_queries == current_amount_of_queries:
                last_amount_of_queries = current_amount_of_queries
                time.sleep(sleep_for)
    except TwitterSearchException as e:
        print(e)
        pass

    print datetime.datetime.now()
    print "Closing the file: ", fo.name
    fo.close()
    #After a connection is reset, it will restart
    print "program ending.....This shows how many times, twitter search had to re-start"
