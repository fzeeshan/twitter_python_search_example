from TwitterSearch import *
import time
from textblob import TextBlob
from gender_detector import GenderDetector
from geopy.geocoders import Nominatim
import datetime

var = 1
while var == 1 :  # This constructs an infinite loop
    detector = GenderDetector() # It can also be ar, uk, uy.
    try:
        tso = TwitterSearchOrder()
        tso.set_keywords(['starbucks', 'red bull', 'dunkin donut', '#starbucks'], or_operator = True)
        tso.set_language('en')
        ts = TwitterSearch(
            consumer_key = 'your twitter consumer key',
            consumer_secret = 'your twitter consumer secret',
            access_token = 'your twitter access token',
            access_token_secret = 'your twitter access token secret'
         )
        sleep_for = 60 # sleep for 60 seconds
        last_amount_of_queries = 0 # used to detect when new queries are done

        for tweet in ts.search_tweets_iterable(tso):
            #initialize variables needed later
            user_lat = None
            user_lon = None
            print "user id:",tweet['user']['id']
            print "Tweet Text:",tweet['text']
            parse_tweet = TextBlob(tweet['text'])
            parse_tweet.sentiment
            polarity = parse_tweet.sentiment.polarity
            subjectivity = parse_tweet.sentiment.subjectivity
            print "polarity:",polarity
            print "subjectivity:",subjectivity
            #polarity sentiment can be changed, using negative < 0, positive >0 neutral = 0
            if (polarity < 0):
                print "negative"
            if (polarity == 0):
                print "neutral"
            if (polarity > 0):
                print "positive"
                
            print "user location:",tweet['user']['location']
            print "screen name:",tweet['user']['screen_name']
            print "user real name:",tweet['user']['name']
            #guess gender
            user_gender = []
            get_name = tweet['user']['name']
            try:        
                index = get_name.find(' ')
                name_for_gender_check = get_name[0:index]
                #debug --- print "found space in name",index
                #debug ---print "grabbed first name",name_for_gender_check
                user_gender = detector.guess(name_for_gender_check)
            except:
                user_gender = 'unknown'
            
            print "user gender is:",user_gender
            print "user language:",tweet['user']['lang']
            print "user time zone:",tweet['user']['time_zone']
            print "user utc_offset:",tweet['user']['utc_offset']
            #find location coordinates try user location if it's available if it doesnt work then check by utc ofset
            
            
            print "user description:",tweet['user']['description']
            print "user create date:",tweet['user']['created_at']
            hashtags = []
            for hashtag in tweet['entities']['hashtags']:
                hashtags.append(hashtag['text'])
            print "hashtags:",str(hashtags).encode('utf-8')
            print "coordinates: ",tweet['coordinates']
            print "geo:",tweet['geo']
            print "tweet id:",tweet['id']
            print "tweet id str:",tweet['id_str']
            print "tweet language:",tweet['lang']
            print "place:",tweet['place']
            print "source:",tweet['source']
            print "tweet created_at:",tweet['created_at']
            if tweet['coordinates'] is not None:
                user_lat = tweet['coordinates']['coordinates'][0]
                user_lon = tweet['coordinates']['coordinates'][1]
                print "found lat lon in the tweet",user_lat,":",user_lon
            else:
                try:
                    if user_lat is None: 
                        get_user_location = tweet['user']['location']
                        geolocator = Nominatim()
                        location = geolocator.geocode(get_user_location)
                        user_lat = location.latitude
                        user_lon = location.longitude
                        print "Grabbed Coordinates using geolocator"
                except:
                    print "error catch"

            print "lat:",user_lat
            print "lon:",user_lon
            print "last_amount_of_queries:",last_amount_of_queries
            current_amount_of_queries = ts.get_statistics()[0]
            if not last_amount_of_queries == current_amount_of_queries:
                last_amount_of_queries = current_amount_of_queries
                time.sleep(sleep_for)
    except TwitterSearchException as e:
        print(e)
        pass

    print datetime.datetime.now()
    print "program ending....."
