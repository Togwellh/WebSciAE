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
import math
import itertools

# Turn the tweet object into the format needed for clustering
def tweetToJSONTweet(oldTweet):
    retweeted = None
    if hasattr(oldTweet,'retweeted_status'):
        retweeted = oldTweet.retweeted_status.user.id_str
    quoted = None
    if hasattr(oldTweet,'quoted_status'):
        quoted = oldTweet.quoted_status.user.id_str
    reply = None
    if hasattr(oldTweet,'in_reply_to_user_id_str'):
        reply = oldTweet.in_reply_to_user_id_str
    usermentions = []
    hashtags = []
    if hasattr(oldTweet,'entities'):
        if hasattr(oldTweet,'user_mentions'):
            for u in oldTweet.entities.user_mentions:
                usermentions.append(u.id_str)
        if hasattr(oldTweet,'hashtags'):
            for u in oldTweet.entities.hashtags:
                hashtags.append(u.text)
    return {"_id":oldTweet.id, "text":oldTweet.text, "createdAt":oldTweet.created_at, "user":oldTweet.user.id_str, "hashtags":hashtags, "mentions":usermentions, "retweeted":retweeted, "quoted":quoted, "reply":reply}

# Sends a rest request for a set keyword
def doRest(auth, query, collection):
    api = tweepy.API(auth)
    dups = 0
    for status in tweepy.Cursor(api.search, q=query, wait_on_rate_limit=True,wait_on_rate_limit_notify=True).items(500): # Requests 500 items
        if (not storeTweet(collection, status)):
            dups += 1
    return dups
    
# Stores the tweet in local mongoDB
def storeTweet(collection, tweetObj):
    tweet = tweetToJSONTweet(tweetObj)
    if tweet == False:
        return False
    try:
        collection.insert_one(tweet)
        return True
    except pymongo.errors.DuplicateKeyError:
        return False

# Connect to the local mongoDB
try:
    client = pymongo.MongoClient("mongodb://user:password@127.0.0.1:27017")
    dtb  = client["test"]
    print("Connected successfully!!!")
except pymongo.errors.ConnectionFailure:
    print ("Could not connect to server: %s" % pymongo.errors.ConnectionFailure)

col = dtb["tweets"]

topUsers = {}
topHashtags = {}

string = dumps(col.find())
jsonObj = json.loads(string)

# Finds the top users and hashtags to determine what the REST probes will be
for j in jsonObj:
    user = j["user"]
    for men in j["mentions"]:
        if men != user:
            if men in topUsers:
                topUsers[men] += 1
            else:
                topUsers[men] = 1
    if 'retweeted' in j:
        if j['retweeted'] != user and j['retweeted'] != None and j['retweeted'] != 'None':
            if j['retweeted'] in topUsers:
                topUsers[j['retweeted']] += 1
            else:
                topUsers[j['retweeted']] = 1
                
    if 'quoted' in j:
        if j['quoted'] != user and j['quoted'] != None and j['quoted'] != 'None':
            if j['quoted'] in topUsers:
                topUsers[j['quoted']] += 1
            else:
                topUsers[j['quoted']] = 1
                
    if 'reply' in j:
        if j['reply'] != user and j['reply'] != None and j['reply'] != 'None':
            if j['reply'] in topUsers:
                topUsers[j['reply']] += 1
            else:
                topUsers[j['reply']] = 1
                
    for ht in j["hashtags"]:
        if ht in topHashtags:
            topHashtags[ht] += 1
        else:
            topHashtags[ht] = 1
                
topUsers = sorted(topUsers.items(), key=lambda x: x[1])[-5:]
topHashtags = sorted(topHashtags.items(), key=lambda x: x[1])[-5:]

# Authenticate with twitter
consumer_key = "x9CIMRSB3JKj8l0GRQ61OMjrO"
consumer_secret = "0c3eDHhZAkjPit78clkjkerchzpr5M7KIeUVZmVwAJnxlsHfSX"
access_key = "1237097610881744896-Js3bZ4QgefEtiMeaHLGW1ba7QQCWaG"
access_secret = "Fgnd8J36tM9ryylFtqbazsVqPJISEspshh0RRyh7uA2ps"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

api = tweepy.API(auth)

# Send the REST probes
for u in topUsers:
    name = api.get_user(u[0]).screen_name
    if name is not None and name != "":
        doRest(auth, str(name), col)

for ht in topUsers:
    if ht[0] is not None and ht[0] != "":
        doRest(auth, str(ht[0]), col)