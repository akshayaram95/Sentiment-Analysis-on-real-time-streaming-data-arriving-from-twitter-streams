import json
import tweepy
import socket
import re
import requests
from datetime import datetime

ACCESS_TOKEN = 'YOUR ACCESS TOKEN'
ACCESS_SECRET = 'YOUR ACCESS SECRET TOKEN'
CONSUMER_KEY = 'YOUR CONSUMER TOKEN'
CONSUMER_SECRET = 'YOUR CONSUMER SECRET KEY'
geo_coding_api_key = 'GOOGLE API KEY'
url = 'https://maps.googleapis.com/maps/api/geocode/json'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
hashtag = '#guncontrolnow'
TCP_IP = 'localhost'
TCP_PORT = 9001
# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print('---------------------')
        processed_String = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",status.text).split())
        processed_String = ' '.join(processed_String.split('\n'))
        if processed_String != '':
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                params = {'key': geo_coding_api_key, 'address': status.user.location}
                r = requests.get(url, params=params)
                results = r.json()['results']
                location = results[0]['geometry']['location']
                new_str=processed_String+"~"+str(location['lat'])+"~"+str(location['lng'])+"~"+timestamp+"\n"
            except:
                new_str=processed_String+"~"+"~"+"~"+timestamp+"\n"
            print(new_str)
            conn.send(new_str.encode('utf-8'))

def on_error(self, status_code):
    if status_code == 420:
        return False
    else:
        print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag])