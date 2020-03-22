from __future__ import print_function
from __future__ import division
import tweepy
import csv
from datetime import datetime
import pymongo
import os
import time
import pandas as pd
import numpy as np
from sklearn.cluster import MiniBatchKMeans, DBSCAN
from bson.json_util import dumps
import json

# Streams the data from the twitter API using the keyword "coronavirus"
def doStream(auth, col):
    api = tweepy.API(auth)
    myStreamListener = MyStreamListener(api=tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True))
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    myStreamListener.set_time(col)
    myStream.filter(track=['coronavirus'], is_async=True)

# A custom streamListener which stores the tweets when they are received
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        storeTweetStream(self.col, status._json)

    def set_time(self, col):
        self.starttime = datetime.now()
        self.col = col

    def on_error(self, status_code):
        print(status_code)
        
# Turns the tweet object into the format required for later clustering
def tweetToJSONTweetStream(oldTweet):
    retweeted = None
    if 'retweeted_status' in oldTweet:
        retweeted = oldTweet['retweeted_status']['user']['id_str']
    quoted = None
    if 'quoted_status' in oldTweet:
        quoted = oldTweet['quoted_status']['user']['id_str']
    reply = None
    if 'in_reply_to_user_id_str' in oldTweet:
        reply = oldTweet['in_reply_to_user_id_str']
    usermentions = []
    hashtags = []
    for u in oldTweet['entities']['user_mentions']:
        usermentions.append(u['id_str'])
    for u in oldTweet['entities']['hashtags']:
        hashtags.append(u['text'])
    return {"_id":oldTweet['id'], "text":oldTweet['text'], "createdAt":oldTweet['created_at'], "user":oldTweet['user']['id_str'], "hashtags":hashtags, "mentions":usermentions, "retweeted":retweeted, "quoted":quoted, "reply":reply}
        
# Function for storing the tweets in mongoDB
def storeTweetStream(collection, tweetObj):
    tweet = tweetToJSONTweetStream(tweetObj)
    if tweet == False:
        return False
    try:
        collection.insert_one(tweet)
        return True
    except pymongo.errors.DuplicateKeyError:
        return False

# Authenticate with Twitter
consumer_key = "x9CIMRSB3JKj8l0GRQ61OMjrO"
consumer_secret = "0c3eDHhZAkjPit78clkjkerchzpr5M7KIeUVZmVwAJnxlsHfSX"
access_key = "1237097610881744896-Js3bZ4QgefEtiMeaHLGW1ba7QQCWaG"
access_secret = "Fgnd8J36tM9ryylFtqbazsVqPJISEspshh0RRyh7uA2ps"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

# Connects to the local mongoDB
try:
    client = pymongo.MongoClient("mongodb://user:password@127.0.0.1:27017")
    db  = client["test"]
    col = db["tweets"]
    print("Connected successfully")
except pymongo.errors.ConnectionFailure:
    print ("Could not connect to server: %s" % pymongo.errors.ConnectionFailure)
    exit()

# Start the streaming
doStream(auth, col)