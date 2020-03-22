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

# Turns the text of a tweet into a numerical hash
def stringToInt(input):
    return hash(input)

# Takes in a tweet and updates the stored statistics using it
def updateInfo(j, clusterInfo, cluster):
    cluster = str(cluster)
    if cluster not in clusterInfo:
        clusterInfo[cluster] = {"hashtags":{}, "mtns":{}, "rtwt":{}, "quo":{}, "hashtagcount":{}, "usercount":{}, "mTotal":0,"rTotal":0,"qTotal":0, "size":0}
    clusterInfo[cluster]["size"] += 1
    
    # Update the hashtag count
    for ht in j["hashtags"]:
        if ht not in clusterInfo[cluster]["hashtags"]:
            clusterInfo[cluster]["hashtags"][ht] = {}
            clusterInfo[cluster]["hashtagcount"][ht] = 0
        clusterInfo[cluster]["hashtagcount"][ht] += 1
        for htOther in j["hashtags"]:
            if ht == htOther:
                continue
            if htOther in clusterInfo[cluster]["hashtags"][ht]:
                clusterInfo[cluster]["hashtags"][ht][htOther] += 1
            else:
                clusterInfo[cluster]["hashtags"][ht][htOther] = 1
        
    # Update the mentions count
    for m in j["mentions"]:
        if m != j['user']:
            if m not in clusterInfo[cluster]["usercount"]:
                clusterInfo[cluster]["usercount"][m] = 1
            else:
                clusterInfo[cluster]["usercount"][m] += 1
        if j['user'] not in clusterInfo[cluster]["mtns"]:
            clusterInfo[cluster]["mtns"][j['user']] = {}
        if m not in clusterInfo[cluster]["mtns"][j['user']]:
            clusterInfo[cluster]["mtns"][j['user']][m] = 1
        else:
            clusterInfo[cluster]["mtns"][j['user']][m] += 1
        clusterInfo[cluster]["mTotal"] += 1
         
    
    # Update the retweet count
    if 'retweeted' in j:
        if j['retweeted'] != None:
            if j['retweeted'] != j['user']:
                if j['retweeted'] not in clusterInfo[cluster]["usercount"]:
                    clusterInfo[cluster]["usercount"][j['retweeted']] = 1
                else:
                    clusterInfo[cluster]["usercount"][j['retweeted']] += 1
            if j['user'] not in clusterInfo[cluster]["rtwt"]:
                clusterInfo[cluster]["rtwt"][j['user']] = {}
            if j['retweeted'] not in clusterInfo[cluster]["rtwt"][j['user']]:
                clusterInfo[cluster]["rtwt"][j['user']][j['retweeted']] = 1
            else:
                clusterInfo[cluster]["rtwt"][j['user']][j['retweeted']] += 1
            clusterInfo[cluster]["rTotal"] += 1
            
    # Update the quoted count
    if 'quoted' in j:
        if j['quoted'] != None:
            if j['quoted'] != j['user']:
                if j['quoted'] not in clusterInfo[cluster]["usercount"]:
                    clusterInfo[cluster]["usercount"][j['quoted']] = 1
                else:
                    clusterInfo[cluster]["usercount"][j['quoted']] += 1
            if j['user'] not in clusterInfo[cluster]["quo"]:
                clusterInfo[cluster]["quo"][j['user']] = {}
            if j['quoted'] not in clusterInfo[cluster]["quo"][j['user']]:
                clusterInfo[cluster]["quo"][j['user']][j['quoted']] = 1
            else:
                clusterInfo[cluster]["quo"][j['user']][j['quoted']] += 1
            clusterInfo[cluster]["qTotal"] += 1
            
    # Update the quoted count, as replies are counted as quotes
    if 'reply' in j:
        if j['reply'] != None:
            if j['reply'] != j['user']:
                if j['reply'] not in clusterInfo[cluster]["usercount"]:
                    clusterInfo[cluster]["usercount"][j['reply']] = 1
                else:
                    clusterInfo[cluster]["usercount"][j['reply']] += 1
            if j['user'] not in clusterInfo[cluster]["quo"]:
                clusterInfo[cluster]["quo"][j['user']] = {}
            if j['reply'] not in clusterInfo[cluster]["quo"][j['user']]:
                clusterInfo[cluster]["quo"][j['user']][j['reply']] = 1
            else:
                clusterInfo[cluster]["quo"][j['user']][j['reply']] += 1
            clusterInfo[cluster]["qTotal"] += 1
                
    return clusterInfo

# Connect to the local mongoDB
try:
    client = pymongo.MongoClient("mongodb://user:password@127.0.0.1:27017")
    dtb  = client["test"]
    print("Connected successfully!!!")
except pymongo.errors.ConnectionFailure:
    print ("Could not connect to server: %s" % pymongo.errors.ConnectionFailure)

# Load the stored tweet data
col = dtb["tweets"]
string = dumps(col.find())
jsonObj = json.loads(string)
for j in range(0,len(jsonObj)):
    jsonObj[j]["numText"] = 0
    if "text" in jsonObj[j]:
        jsonObj[j]["numText"] = stringToInt(jsonObj[j]["text"])
    jsonObj[j]["id"] = jsonObj[j]["_id"]
    jsonObj[j]["cluster"] = -1
tweets = pd.DataFrame(jsonObj)

# Start of the clustering code from GitHub

# Set index to id for easy matching
tweets.set_index('_id', inplace=True)

# Start timing implementation
t0 = time.time()

# MiniBatch section
mb = MiniBatchKMeans(n_clusters=10, init='k-means++', n_init=10, batch_size=5000)
#data = tweets.as_matrix(columns=['numText'])
data = tweets[["numText"]].values
mb.fit(data)
tweets['mb_cluster'] = mb.labels_   # Add labels back into DataFrame

# DBSCAN section
meters = 100      # Transform meters to degrees (roughly)
eps = meters / 100000

eps =1000000000000000000

for i in tweets.mb_cluster.unique():
    subset = tweets.loc[tweets.mb_cluster == i]
    db = DBSCAN(eps=eps, min_samples=100)
    #data = subset.as_matrix(columns=['numText'])
    data = subset[["numText"]].values
    db.fit(data)
    subset['db_cluster'] = db.labels_
    tweets.loc[tweets.mb_cluster == i, 'db_cluster'] = subset['db_cluster']

# Set final cluster variable
tweets['cluster'] = tweets.mb_cluster + (tweets.db_cluster.replace(-1.0, np.nan) / 100)

clustersize = len(tweets.cluster.unique())
t1 = time.time() - t0

# Turn the results of clustering into a usable format
clustered_tweets = {}
tweets = tweets.to_dict()
counter = 0
id_to_cluster = {}
count_to_cluster = {}

for cluster in tweets['cluster']:
    counter+=1
    if math.isnan(tweets['cluster'][cluster]):
        continue
    count_to_cluster[counter] = int(tweets['cluster'][cluster])
counter = 0

for id in tweets['id']:
    counter+=1
    if counter in count_to_cluster:
        id_to_cluster[int(id)] = count_to_cluster[counter]
        
for j in range(0,len(jsonObj)):
    if jsonObj[j]["_id"] in id_to_cluster:
        jsonObj[j]["cluster"] = id_to_cluster[jsonObj[j]["_id"]]

# Authenticate the twitter API
consumer_key = "x9CIMRSB3JKj8l0GRQ61OMjrO"
consumer_secret = "0c3eDHhZAkjPit78clkjkerchzpr5M7KIeUVZmVwAJnxlsHfSX"
access_key = "1237097610881744896-Js3bZ4QgefEtiMeaHLGW1ba7QQCWaG"
access_secret = "Fgnd8J36tM9ryylFtqbazsVqPJISEspshh0RRyh7uA2ps"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

api = tweepy.API(auth)

clusterInfo = {}

# Call the updateInfo function for each tweet
for j in jsonObj:
    results = {}
    cluster = j["cluster"]
    
    if not math.isnan(cluster):
        clusterInfo = updateInfo(j, clusterInfo, cluster)
    
    cluster = -1 # -1 cluster is for general twitter data not just for a specific cluster
    clusterInfo = updateInfo(j, clusterInfo, cluster)
    
# Find tbe top hashtags, top users and number of triads
for c in clusterInfo:

    mtnsTriads = []
    rtwtTriads = []
    quoTriads = []

    topHashtags = sorted(clusterInfo[c]["hashtagcount"].items(), key=lambda x: x[1])[-5:]
    topUsers = sorted(clusterInfo[c]["usercount"].items(), key=lambda x: x[1])[-5:]
    
    userStr = ""
    for u in topUsers:
        userStr += api.get_user(u[0]).screen_name + ", "

    userStr = userStr[:-2]
    
    hashStr = ""
    for h in topHashtags:
        hashStr += h[0] + ", "
    hashStr = hashStr[:-2]
    
    clusterInfo[c]["topHashtags"] = hashStr
    clusterInfo[c]["topUsers"] = userStr

    # Find the triads for mentions
    for m in clusterInfo[c]["mtns"]:
        # m is initial
        for mentioned in clusterInfo[c]["mtns"][m]:
            if mentioned == m:
                continue
            #mentioned is person mentioned by m
            if mentioned in clusterInfo[c]["mtns"]:
                for ment in clusterInfo[c]["mtns"][mentioned]:
                    if ment == mentioned or ment == m:
                        continue
                    toAdd = str(m) + " -> " + str(mentioned) + " -> " + str(ment)
                    if toAdd not in mtnsTriads:
                        mtnsTriads.append(toAdd)
                    
    # Find the triads for retweets
    for r in clusterInfo[c]["rtwt"]:
        for rtwted in clusterInfo[c]["rtwt"][r]:
            if rtwted == r:
                continue
            if rtwted in clusterInfo[c]["rtwt"]:
                for rtwt in clusterInfo[c]["rtwt"][rtwted]:
                    if rtwt == rtwted or rtwt == r:
                        continue
                    toAdd = str(r) + " -> " + str(rtwted) + " -> " + str(rtwt)
                    if toAdd not in rtwtTriads:
                        rtwtTriads.append(toAdd)
                        
    # Find the triads for quotes/replies
    for q in clusterInfo[c]["quo"]:
        for quoted in clusterInfo[c]["quo"][q]:
            if quoted == q:
                continue
            if quoted in clusterInfo[c]["quo"]:
                for quo in clusterInfo[c]["quo"][quoted]:
                    if quo == quoted or quo == q:
                        continue
                    toAdd = str(q) + " -> " + str(quoted) + " -> " + str(quo)
                    if toAdd not in quoTriads:
                        quoTriads.append(toAdd)
                    
    clusterInfo[c]["mtnsTriads"] = mtnsTriads
    clusterInfo[c]["rtwtTriads"] = rtwtTriads
    clusterInfo[c]["quoTriads"] = quoTriads

avg = 0
min = 99999999
max = 0

# Print the data
for c in clusterInfo:
    print(" ")
    cStr = c
    if cStr == '-1':
        cStr = "Overall : "
    else:
        cStr = "Cluster : " + c
        
    print(cStr + " with size " + str(clusterInfo[c]["size"]))

    if c != "-1":
        if (clusterInfo[c]["size"] > max):
            max = clusterInfo[c]["size"]
            
        if (clusterInfo[c]["size"] < min):
            min = clusterInfo[c]["size"]
    
        avg += clusterInfo[c]["size"]
    if ("topUsers" in clusterInfo[c]):
        print("Top users : " + clusterInfo[c]["topUsers"])
    if ("topHashtags" in clusterInfo[c]):
        print("Top hashtags : " + clusterInfo[c]["topHashtags"])
    print("User ties = Mentions : " + str(clusterInfo[c]["mTotal"]) + " Retweets : " + str(clusterInfo[c]["rTotal"]) + " Quotes/Replies : " + str(clusterInfo[c]["qTotal"]))    
    print("User triads = Mentions : " + str(len(clusterInfo[c]["mtnsTriads"])) + " Retweets : " + str(len(clusterInfo[c]["rtwtTriads"])) + " Quotes/Replies : " + str(len(clusterInfo[c]["quoTriads"])))

avg = avg / clustersize
print(" ")
print(str(clustersize) + " clusters were created, with average size of " + str(avg) + " the largest was size " + str(max) + " and the smallest was size " + str(min))
print("Total number of tweets is " + str(len(jsonObj)))

# Store the results in the local mongoDB
dtb["results"].drop()
dtb["results"].insert_one(clusterInfo)    