API
===
#!/usr/bin/python

import httplib2
import os
import sys
import time

import MySQLdb

import gdata.youtube
import gdata.youtube.service

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow
from gdata.youtube import service

USERNAME = 'weichung.hsu@gmail.com'
PASSWORD = 'b6506051$'
VIDEO_ID = '9bZkp7q19f0'
comment_counter = 0

class CommentStruct():
	url = ''
	author = ''
	uri = ''
	updated = ''
	content = ''
	userID = ''


def TraceDBComment(client, args, db, myLog, youtube):
    global comment_counter
    
    video_id = args.video_id    
    cur = db.cursor()
     mysql_cmd = 'show tables like "' +video_id+ '"'
    found_table = 0
    end_of_comment = 0
    id = ''
    last_post = ''
    myComment = CommentStruct()
    
    cur.execute(mysql_cmd)
    if cur.fetchone() is not None:
    	found_table = 1
    
    mysql_cmd = "SET NAMES 'utf8mb4';"
    cur.execute(mysql_cmd)
    db.commit()
    mysql_cmd = 'SET CHARACTER SET utf8mb4;'
    cur.execute(mysql_cmd)
    db.commit()
    
    db.set_character_set('utf8')
    cur.execute('SET NAMES utf8mb4;')
    cur.execute('SET CHARACTER SET utf8mb4;')
    cur.execute('SET character_set_connection=utf8mb4;')
    
    cur.execute("ALTER DATABASE `youtube_comments` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'")
    db.commit()
    
    #add video table if it doesn't exist
    mysql_cmd = 'show tables like "video_table"'
    cur.execute(mysql_cmd)
    if cur.fetchone() is None:
    	mysql_cmd = "create table `video_table` (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, vid VARCHAR(15), title VARCHAR(300), published VARCHAR(30), tags VARCHAR(300), duration VARCHAR(20), likeCount int, dislikeCount int, view_count bigint, description text);"
    	cur.execute(mysql_cmd)
    	db.commit()
    	mysql_cmd = "ALTER TABLE `video_table` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()
    else:
    	mysql_cmd = "ALTER TABLE `video_table` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()
    
    #add user table if it doesn't exist
    mysql_cmd = 'show tables like "user_table"'
    cur.execute(mysql_cmd)
    if cur.fetchone() is None:
    	mysql_cmd = "create table `user_table` (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, userID VARCHAR(26), uploaded_number int, uploaded_vid_text text, subscription_number int, subscription_table_list text);"
    	cur.execute(mysql_cmd)
    	db.commit()
    	mysql_cmd = "ALTER TABLE `user_table` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()
    else:
    	mysql_cmd = "ALTER TABLE `user_table` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()
    	
    #add subscription table if it doesn't exist
    mysql_cmd = 'show tables like "subscription_table"'
    cur.execute(mysql_cmd)
    if cur.fetchone() is None:
    	mysql_cmd = "create table `subscription_table` (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, display_name VARCHAR(128) );"
    	cur.execute(mysql_cmd)
    	db.commit()
    	mysql_cmd = "ALTER TABLE `subscription_table` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()
    else:
    	mysql_cmd = "ALTER TABLE `subscription_table` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()

    if found_table == 1:
    	mysql_cmd = 'select * from `' + video_id + '`'
    	cur.execute(mysql_cmd)
    	for row in cur.fetchall() :
    		last_post = row[1]
    		if last_post == "N":
    			id = row[0]
    			myComment.url = row[2]
    			myComment.author = row[3]
    			myComment.uri = row[4]
    			myComment.userID = row[5]
    			myComment.updated = row[6]
    			myComment.content = row[7]
    			
    			myLog.write('comment #'+str(comment_counter)+':\n')
    				
    			if myComment.url is not None:
    				myLog.write(myComment.url+'\n')
    			if myComment.author is not None:
    				myLog.write(myComment.author+'\n')
    			if myComment.uri is not None:
    				myLog.write(myComment.uri+'\n')
    			if myComment.updated is not None:
    				myLog.write(myComment.updated+'\n')
    			if myComment.content is not None:
    				myLog.write(myComment.content+'\n')
    			comment_counter = comment_counter + 1
    		else:
    			end_of_comment = 1
    			break

    	if end_of_comment == 0:
    		mysql_cmd = "ALTER TABLE `" + video_id + "` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    		cur.execute(mysql_cmd)
    		db.commit()
    		args.url = myComment.url
    		GetCommentFeed(client, args, db, myLog, youtube)
    		args.url = None
    		    	
    else:    	
    	mysql_cmd = "create table `" + video_id + "` (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, last_post CHAR(1), url VARCHAR(300), author VARCHAR(100), uri VARCHAR(80), userID VARCHAR(26), updated VARCHAR(30), content mediumtext);"
    	cur.execute(mysql_cmd)
    	db.commit()
    	mysql_cmd = "ALTER TABLE `" + video_id + "` convert to character set 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
    	cur.execute(mysql_cmd)
    	db.commit()
    	
    	#weichung hsu, todo here
    	#check if this video is in DB or not, skip if it is in DB already
    	mysql_cmd = 'select * from video_table where vid="' + video_id + '";'
    	cur.execute(mysql_cmd)
    	if cur.fetchone() is None: #not found
    		yt_stat = youtube.videos().list(id=video_id, part='statistics').execute()
    		yt_snippet = youtube.videos().list(id=video_id, part='snippet').execute()
    		yt_cont = youtube.videos().list(id=video_id, part='contentDetails').execute()
    		
    		mysql_cmd = 'insert into `video_table` (`id`, `vid`,`title`,`published`,`tags`,`duration`, `likeCount`, `dislikeCount`, `view_count`, `description`) VALUES  (NULL, "' + video_id + '", '
    		mysql_cmd = mysql_cmd + '"'  + yt_snippet['items'][0]['snippet']['title'].replace("\"", "`") + ' ", '
    		mysql_cmd = mysql_cmd + '"'  + yt_snippet['items'][0]['snippet']['publishedAt'] + '", '
    		
    		try:
    			mysql_cmd = mysql_cmd + '"'  + yt_snippet['items'][0]['snippet']['tags'].replace("\"", "`") + '", '
    		except KeyError:
    			mysql_cmd = mysql_cmd + 'NULL, '
    		
    		try:
    			mysql_cmd = mysql_cmd + '"'  + yt_cont['items'][0]['contentDetails']['duration'] + '", '
    		except KeyError:
    			mysql_cmd = mysql_cmd + '0, '
    		
    		mysql_cmd = mysql_cmd + '"'  + str(yt_stat['items'][0]['statistics']['likeCount']) + '", '
    		mysql_cmd = mysql_cmd + '"'  + str(yt_stat['items'][0]['statistics']['dislikeCount']) + '", '
    		mysql_cmd = mysql_cmd + '"'  + str(yt_stat['items'][0]['statistics']['viewCount']) + '", '
    		mysql_cmd = mysql_cmd + '"' + yt_snippet['items'][0]['snippet']['description'].replace("\"", "`") + ' ");'
    		cur.execute(mysql_cmd)
        	db.commit()


class UserStruct():
	userID = ''
	uploaded_num = 0
	uploaded_vid_text = ''
	favorite_num = 0
	favorite_vid_text = ''
	playlist_num = 0
	playlist_text = ''
	subscription_number = 0
	subscription_table_list = ''


#create table `video_table` 
def AddVideoFeed(cur, db, myUser, feed, video_type, youtube):
    for entry in feed.entry:
    	entry_vid = ''
    	if entry.media is not None:
    		if entry.media.player is not None: #if entry.media.player.url is not None:
    			try:
    				entry_vid = entry.media.player.url[32:43]
    			except:
    				return
    			
    			if video_type == 'uploaded':
    				#add this uploaded video into user data structure
    				myUser.uploaded_vid_text = myUser.uploaded_vid_text + entry_vid + ' '
    				myUser.uploaded_num = myUser.uploaded_num + 1
    			else:
    				#add this favorite video into user data structure
    				myUser.favorite_vid_text = myUser.favorite_vid_text + entry_vid + ' '
    				myUser.favorite_num = myUser.favorite_num + 1
    			
    			
    			#check if this video is in DB or, skip if it is in DB already
    			mysql_cmd = 'select * from video_table where vid="' + entry_vid + '";'
        		cur.execute(mysql_cmd)
        		if cur.fetchone() is not None: #found
        			continue
        		mysql_cmd = 'insert into `video_table` (`id`, `vid`,`title`,`published`,`tags`,`duration`, `likeCount`, `dislikeCount`, `view_count`, `description`) VALUES  (NULL, "' + entry_vid + '", '
        	else:
        		continue #no video_id, continue
    				
    		if entry.media.title is not None:
    			mysql_cmd = mysql_cmd + '"'  + entry.media.title.text.replace("\"", "`") + ' ", '
    		else:
    			mysql_cmd = mysql_cmd + 'NULL, '

    		if entry.published is not None:
    			mysql_cmd = mysql_cmd + '"'  + entry.published.text + '", '
    		else:
    			mysql_cmd = mysql_cmd + 'NULL, '

    		if entry.media.keywords.text is not None:
    			mysql_cmd = mysql_cmd + '"'  + entry.media.keywords.text.replace("\"", "`") + '", '
    		else:
    			mysql_cmd = mysql_cmd + 'NULL, '
    		
    		if entry.media.duration is not None:
    			mysql_cmd = mysql_cmd + '"'  + entry.media.duration.seconds + '", '
    		else:
    			mysql_cmd = mysql_cmd + 'NULL, '

    		if entry.statistics is not None:
    			yt_stat = youtube.videos().list(id=entry_vid, part='statistics').execute()
    			mysql_cmd = mysql_cmd + '"'  + str(yt_stat['items'][0]['statistics']['likeCount']) + '", '
    			mysql_cmd = mysql_cmd + '"'  + str(yt_stat['items'][0]['statistics']['dislikeCount']) + '", '
    			mysql_cmd = mysql_cmd + '"'  + str(entry.statistics.view_count) + '", '
    		else:
    			mysql_cmd = mysql_cmd + '0, 0, 0, '

    		if entry.media.description.text is not None:
    			mysql_cmd = mysql_cmd + '"' + entry.media.description.text.replace("\"", "`") + ' ");'
    		else:
    			mysql_cmd = mysql_cmd + 'NULL);'

    		cur.execute(mysql_cmd)
        	db.commit()
        	
        	if entry.geo is not None:
        		geo_location = entry.geo.location()
        		#if geo_location is not None:
        		#	print(geo_location)
        		#else:
        		#	print('geo is ok, but location is None')
        	#else:
        	#	print('geo is None')


def AddUserDB(client, cur, db, userID, youtube):
    #check if this video is in DB or, skip if it is in DB already
    mysql_cmd = 'select * from user_table where userID="' + userID + '";'
    cur.execute(mysql_cmd)
    if cur.fetchone() is not None: #found
    	return
    if userID=='__NO_YOUTUBE_ACCOUNT__':
    	return
        
    myUser = UserStruct()
    myUser.userID = userID
    
    #1. user upload info
    uri_link = 'http://gdata.youtube.com/feeds/api/users/' + userID + '/uploads'
    try:
    	feed = client.GetYouTubeVideoFeed(uri=uri_link)
    except:
    	#gdata.service.RequestError
    	#return
    	myUser.uploaded_vid_text = ''
    	myUser.uploaded_num = 0
    else:
    	AddVideoFeed(cur, db, myUser, feed, 'uploaded', youtube)
    
    time.sleep(5)
    #2. favorite
    try:
    	feed = client.GetUserFavoritesFeed(username=userID)
    except:
    	myUser.favorite_vid_text = ''
    	myUser.favorite_num = 0
    else:
    	AddVideoFeed(cur, db, myUser, feed, 'favorite', youtube)
        
    #3. playlist
    try:
    	myPlaylist = client.GetYouTubePlaylistFeed(username=userID)
    except:
    	myUser.playlist_num = 0
    	myUser.playlist_vid_text = ''
    else:
    	# iterate through the feed as you would with any other
    	for playlist_video_entry in myPlaylist.entry:
    		if playlist_video_entry.title is not None:
    			myUser.playlist_text = myUser.playlist_text + ' ' + playlist_video_entry.title.text
    			myUser.playlist_num = myUser.playlist_num + 1
    
    #4. subscription
    try:
    	subscription_feed = client.GetYouTubeSubscriptionFeed(username=userID)
    except:
    	subscription_feed = None
    	
    if isinstance(subscription_feed, gdata.youtube.YouTubeSubscriptionFeed):
    	# given a YouTubeSubscriptionEntry we can determine it's type (channel, favorite, or query)
    	for entry in subscription_feed.entry:
    		subscription_str = entry.ToString()
    		start_index = subscription_str.find("username display") + 18
    		subscription_str = subscription_str[start_index:]
    		end_index = subscription_str.find('">')
    		subscription_str = subscription_str[:end_index]
    		mysql_cmd = 'select * from subscription_table where display_name="' + subscription_str + '";'
    		cur.execute(mysql_cmd)
    		entry_list = cur.fetchone()
    		if entry_list is not None: #found
    			id = entry_list[0]
    		else:
    			mysql_cmd = 'insert into `subscription_table` (`id`, `display_name`) VALUES  (NULL, "' + subscription_str + '");'   
    			cur.execute(mysql_cmd)
    			db.commit()
    			mysql_cmd = 'select * from subscription_table where display_name="' + subscription_str + '";'
    			cur.execute(mysql_cmd)
    			id = cur.fetchone()[0]
    		
    		myUser.subscription_number = myUser.subscription_number + 1
    		myUser.subscription_table_list = myUser.subscription_table_list + str(id) + ' '
    else:
    	myUser.subscription_table_list = ''
    	myUser.subscription_number = 0
    
    mysql_cmd = 'insert into `user_table` (`id`, `userID`, `uploaded_number`, `uploaded_vid_text`, `subscription_number`, `subscription_table_list`) VALUES  (NULL, "' + myUser.userID + '", "' + str(myUser.uploaded_num) + '", "' + myUser.uploaded_vid_text +  '", "'+ str(myUser.subscription_number) + '", "' + myUser.subscription_table_list + '");'
    cur.execute(mysql_cmd)
    db.commit()


comment_feed_url = "http://gdata.youtube.com/feeds/api/videos/%s/comments?orderby=published&max-results=50"
def GetCommentFeed(client, args, db, myLog, youtube):
    global comment_counter
    update_latest_comment = 0
    found_last_entry = 1
    
    video_id = args.video_id
    if args.url is None:
    	url = comment_feed_url % video_id
    	comment_feed = client.GetYouTubeVideoCommentFeed(uri=url)
    	update_latest_comment = 1
    	found_last_entry = 0 # if just update the new entries, don't update the "last_post" column
    else:
    	url = args.url
    	comment_feed = client.Query(url)
    	update_latest_comment = 0

    counter = 0
    myURL = url
    cur = db.cursor()
    myComment = CommentStruct()
     
    while comment_feed:
        for comment_entry in comment_feed.entry:       	        	
        	myComment.author = comment_entry.author[0].name.text
        	myComment.content = comment_entry.content.text
        	myComment.updated = comment_entry.updated.text
        	myComment.uri = comment_entry.author[0].uri.text
        	myComment.url = myURL
        	myComment.userID = myComment.uri.replace('https://gdata.youtube.com/feeds/api/users/', '')
        	AddUserDB(client, cur, db, myComment.userID, youtube) #weichung hsu, why
        	
        	#1st, check if it is in DB already
        	if myComment.updated is not None:
        		# weichung hsu, need update here, the entry sometime will be same values, but different url
        		mysql_cmd = 'select * from `' + video_id + '` where updated="' + myComment.updated + '" and url="' + myComment.url + '";' 
        		cur.execute(mysql_cmd)
        		if cur.fetchone() is not None:
        			#found
        			if update_latest_comment == 0:
        				#this comment has been in DB already, skip
        				continue
        			else:
        				#this comment has been in DB already, stop updating DB, this is the last update
        				return
        		else:
        			# this entry is new, can't find in DB
        			if update_latest_comment == 0:
        				found_last_entry = 0
        	else:
        		found_last_entry = 0
        	
        	myLog.write('comment #'+str(comment_counter)+':\n')
        	mysql_cmd = 'insert into `' + video_id + '` (`id`, `last_post`,`url`,`author`,`uri`,`userID`,`updated`,`content`) VALUES  (NULL, "N", '
        	
        	if myComment.url is not None:
        		myLog.write(myComment.url+'\n')
        		mysql_cmd = mysql_cmd + '"' + myComment.url + '", ' 
        	else:
        		mysql_cmd = mysql_cmd + '"", '
        	
        	if myComment.author is not None:
        		myLog.write(myComment.author+'\n')
        		mysql_cmd = mysql_cmd + '"' + myComment.author.replace("\"", "`") + '", ' 
        	else:
        		mysql_cmd = mysql_cmd + '"", '
        		        		
        	if myComment.uri is not None:
        		myLog.write(myComment.uri+'\n')
        		mysql_cmd = mysql_cmd + '"' + myComment.uri + '", ' 
        	else:
        		mysql_cmd = mysql_cmd + '"", '
        	
        	if myComment.userID is not None:
        		myLog.write(myComment.uri+'\n')
        		mysql_cmd = mysql_cmd + '"' + myComment.userID + '", ' 
        	else:
        		mysql_cmd = mysql_cmd + '"", '

        	if myComment.updated is not None:
        		myLog.write(myComment.updated+'\n')
        		mysql_cmd = mysql_cmd + '"' + myComment.updated + '", ' 
        	else:
        		mysql_cmd = mysql_cmd + '"", '

        	if myComment.content is not None:
        		myLog.write(myComment.content+'\n')
        		mysql_cmd = mysql_cmd + '"' + myComment.content.replace("\"", "`") + ' ");'
        	else:
        		mysql_cmd = mysql_cmd + '"");'
        		
        	cur.execute(mysql_cmd)
        	db.commit()	
        	comment_counter = comment_counter + 1
        
        if comment_feed.GetNextLink() is None:
        	print("next link is null")
        	break    
        myURL = comment_feed.GetNextLink().href
        time.sleep(5)
        comment_feed = client.Query(myURL)
        print("counter "+str(counter))
        counter = counter + 1
        if counter == 10:
        	break 
    
    if found_last_entry == 1:
    	mysql_cmd = "update " + video_id + " set last_post = 'Y';"
    	#cur.execute(mysql_cmd)
    	#db.commit()



# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the Google Developers Console at
# https://console.developers.google.com/.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#   https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
CLIENT_SECRETS_FILE = "client_secrets.json"

# This variable defines a message to display if the CLIENT_SECRETS_FILE is
# missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0

To make this sample run you will need to populate the client_secrets.json file
found at:

   %s

with information from the Developers Console
https://console.developers.google.com/

For more information about the client_secrets.json file format, please visit:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
""" % os.path.abspath(os.path.join(os.path.dirname(__file__),
                                   CLIENT_SECRETS_FILE))

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account.
YOUTUBE_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/youtube"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def get_authenticated_service(args):
  flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE,
    scope=YOUTUBE_READ_WRITE_SCOPE,
    message=MISSING_CLIENT_SECRETS_MESSAGE)

  storage = Storage("%s-oauth2.json" % sys.argv[0])
  credentials = storage.get()

  if credentials is None or credentials.invalid:
    credentials = run_flow(flow, storage, args)

  return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    http=credentials.authorize(httplib2.Http()))




if __name__ == "__main__":
  argparser.add_argument("--url",
    help="URL link")
  argparser.add_argument("--video-id", required=True,
    help="Optional ID of video to post.")
  argparser.add_argument("--playlist-id",
    help="Optional ID of playlist to post.")
  args = argparser.parse_args()

  # You can post a message with or without an accompanying video or playlist.
  # However, you can't post a video and a playlist at the same time.
  if args.video_id and args.playlist_id:
    exit("You cannot post a video and a playlist at the same time.")

  db = MySQLdb.connect(host="localhost", user="root", passwd="b6506051", db="youtube_comments")
  try:
    client = service.YouTubeService()
    client.ssl = True
    client.ClientLogin(USERNAME, PASSWORD)
    comment_counter = 0
    myLogName = args.video_id + '.log'
    myLog = open(myLogName,"w")
    
    youtube = get_authenticated_service(args)

    TraceDBComment(client, args, db, myLog, youtube)    
    GetCommentFeed(client, args, db, myLog, youtube)
    
    myLog.close()

        
  except HttpError, e:
    print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
  else:
    print "Get all comments."






    
