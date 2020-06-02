import tweepy, json, os, sys, pymongo

# In the mongo shell, to clear:
#  mongo < clear.js


count = 0
client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = client["my_db"]
my_col = my_db["tweets"]

# only collect info we might need
def filter_tweet(tweet):
	user = tweet["user"]
	if "extended_tweet" in tweet:
		text = tweet["extended_tweet"]["full_text"]
	else:
		text = tweet["text"]
	rtn = {
		"created_at":tweet["created_at"],
		"id_str":tweet["id_str"],
		"source":tweet["source"],
		"coordinates":tweet["coordinates"],
		"timestamp_ms":tweet["timestamp_ms"],
		"lang":tweet["lang"],
		"text":text,
		"user":{
			"id_str":user["id_str"],
			"location":user["location"],
			"name":user["name"],
			"profile_image_url":user["profile_image_url"],
			"screen_name":user["screen_name"],
			"statuses_count":user["statuses_count"],
			"verified":user["verified"],
		}
	}

	return rtn

def print_db():
	global my_col
	for p in my_col.find():
		print(p["text"])

class MyStreamListener(tweepy.StreamListener):
	def on_status(self, status):
		global my_col, count
		tweet = status._json
		tweet = filter_tweet(tweet)
		
		# skip retweets
		if "retweeted_status" in tweet or "RT" in tweet["text"]:
			return True

		# print(f"{tweet['text']}", file=sys.stderr)
		my_col.insert_one(tweet)
		print("inserted")
		count += 1
		count = count % 10
		if count == 9:
			stats = my_db.command("dbstats")
			print(float(stats["dataSize"])/float(1<<20))
			# print(my_col.stats(1024 * 1024))


		return(True)

	def on_error(self, status_code):
		print(f"Error: {status_code}")

auth_details = {}

def set_auth():
	global auth_details
	secrets = ["/Users/joey/secrets/twitter_creds"]
	for s in secrets:
		if os.path.exists(s):
			with open(s) as inf:
				auth_details = json.load(inf)
				break

set_auth()
# print(auth_details)
# sys.exit(9)
# auth = tweepy.AppAuthHandler(auth_details["api_key"], auth_details["api_secret"])
# api = tweepy.API(auth)

auth = tweepy.OAuthHandler(auth_details["api_key"],auth_details["api_secret"])
auth.set_access_token(auth_details["token"],auth_details["token_secret"])

my_listener = MyStreamListener()
stream = tweepy.Stream(auth=auth, listener=my_listener)
# stream.filter(track=["trump"], is_async=True)
stream.filter(track=["corona"])

# while True:
# 	pass

# import pymongo



