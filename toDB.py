#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json,os, sys
import sqlite3 as lite

Json_files = os.listdir('data')
finished = []

for filename in Json_files:
    toDB = []
    if prefix in filename:
        with open('data/'+filename) as f:
            for line in f:
                try:
                    data = json.loads(line)
                    rows = (data['id'],unicode(data['text']),data['user']['id'],unicode(data['created_at']),unicode(data['place']['name']),unicode(data['place']['country_code']),data['geo']['coordinates'][0],data['geo']['coordinates'][1],unicode(data['entities']['hashtags']),unicode(data['lang']),data['truncated'],unicode(data['source']))
                    toDB.append(rows)
                except:
                    pass
        con = lite.connect('tweets.db')
        with con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS Tweets")
            cur.execute("CREATE TABLE Tweets(tweet_id INT, tweet_text TEXT, user_id INT, created_at TEXT, place TEXT, country_code TEXT, latitude REAL, longitude REAL, hashtags TEXT, language TEXT, truncated BOOLEAN, source TEXT)")
            cur.executemany("INSERT INTO Tweets_till_7_16_14 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",tuple(toDB))
        print filename, " to DB done\n\n"
    
