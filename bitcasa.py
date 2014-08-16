# Bitcasa Python Class (Unofficial)
# Michael Thomas, 2013 
# TODO #
########
# Test for Python2 & Python3
# Error Handling that works well.
# Implement File Methods
# DONE: download file to cache
# upload file from cache

# Import Section
import urllib2, urllib, urllib3
import json
import base64
import sys
import os
import pprint
import httplib
import socket
import time
from urlparse import urlparse
import mutex
import threading
import datetime
from collections import namedtuple

# Upload Progress Helper
# @todo - May switch Download to this as well.
class Progress(object):
	def __init__(self):
		self._seen = 0.0

	def update(self, total, size, name):
		self._seen += size
		pct = (self._seen / total) * 100.0
		print '%s progress: %.2f' % (name, pct)

class file_with_callback(file):
	def __init__(self, path, mode, callback, *args):
		file.__init__(self, path, mode)
		self.seek(0, os.SEEK_END)
		self._total = self.tell()
		self.seek(0)
		self._callback = callback
		self._args = args

	def __len__(self):
		return self._total

	def read(self, size):
		print "file_with_callback.read"
		data = file.read(self, size)
		self._callback(self._total, len(data), *self._args)
		return data

# Bitcasa Class
class Bitcasa:
	httpsPool = None
	retry = urllib3.util.Retry(read=3, backoff_factor=2)
	# Start Client & Load Config
	def __init__ (self, config_path):
		# Config file
		self.config_path = config_path
		try:
			with open(self.config_path, 'r') as config_file:
				self.config = json.load(config_file)
		except:
			sys.exit("Could not find configuration file.")
		# Set configuation variables
		self.api_url = self.config['api_url'].encode('utf-8')
		self.client_id = self.config['client_id'].encode('utf-8')
		self.secret = self.config['secret'].encode('utf-8')
		self.redirect_url = self.config['redirect_url'].encode('utf-8')
		self.auth_token = self.config['auth_token'].encode('utf-8')
		self.access_token = self.config['access_token'].encode('utf-8')
		
		# Adding support for File Cache
		self.cache_dir = self.config['cache_dir'].encode('utf-8')
		# Check to see if cache_dir is valid & exists
		if(self.cache_dir == None):
			self.cache_dir = os.path.dirname(os.path.realpath(__file__)) + ".cache"
			self.save_config()
		# Now Make sure it exists
		if not os.path.exists(self.cache_dir):
			os.makedirs(self.cache_dir)
		
		# See if we need our tokens
		if(self.auth_token == "") or (self.access_token == ""):
			return self.authenticate()

		# Initiate Connection
		self.api_host = urlparse(self.api_url).hostname
		self.api_path = urlparse(self.api_url).path
		# Create Connection Pool
		urllib3.connection.HTTPSConnection.default_socket_options + [
			(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
		]
		urllib3.connection.HTTPConnection.default_socket_options + [
                        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
                ]

		self.httpsPool = urllib3.HTTPSConnectionPool(self.api_host, maxsize=10, timeout=urllib3.Timeout(connect=1.0, read=2.0))

		return None

	def save_config (self):
		with open(self.config_path, 'w') as outfile:
			json.dump(self.config, outfile, indent=4)

	def authenticate (self):
		print("### ENTER THE FOLLOWING URL IN A BROWSER AND AUTHORIZE THIS APPLICATION ###")
		print(self.api_url + "/oauth2/authenticate?client_id=" + self.client_id + "&redirect=" + self.redirect_url)
		print("### ONCE YOU HAVE AUTHORIZED THE APPLICATION, ENTER THE AUTH TOKEN HERE (WILL BE IN URL) ###")
		auth = raw_input("Auth Token: ")
		self.auth_token = auth
		self.config['auth_token'] = self.auth_token
		request = urllib2.Request("https://developer.api.bitcasa.com/v1/oauth2/access_token?secret="+ self.secret +"&code=" + self.auth_token)
		try:
			response = json.load(urllib2.urlopen(request))
			self.access_token = response['result']['access_token']
			self.config['access_token'] = self.access_token
			self.save_config()
			return True
		except urllib2.HTTPError, e:
			error = e.read()
			return error

	def list_folder (self, path = ""):
		print "Threads: "+str(threading.active_count())+" "+str(threading.current_thread().ident)
		r2 = None
		response = None
		try:
			list_folder_url = self.api_path + "/folders" + path + "?access_token=" + self.access_token
			print "list_folder.list_folder_url = "+list_folder_url
			pool_start = datetime.datetime.now()
			r2 = self.httpsPool.request("GET", list_folder_url,retries=self.retry)
			pool_end = datetime.datetime.now()
			diff = pool_end - pool_start
			print "list_folder: "+str(r2.status)+" time (secs):"+str(diff.seconds)+"."+str(diff.microseconds)
			data_start = datetime.datetime.now()
			raw_response = r2.data
			data_end = datetime.datetime.now()
			diff = data_end - data_start
			print "list_folder.raw_response = "+raw_response+" time(secs):"+str(diff.seconds)+"."+str(diff.microseconds)
			response = json.loads(raw_response)
		except Exception, e:
			print "Exception: "+str(e)
			return {}
		if(response['result'] == None):
			return response
		else:
			return response['result']['items']

	def add_folder (self, path, folder_name):
		payload = {"folder_name":folder_name}
		request = urllib2.Request(self.api_url + "/folders/" + path + "?access_token=" + self.access_token, urllib.urlencode(payload))
		try:
			response = json.load(urllib2.urlopen(request))
		except httplib2.HttpLib2Error, e:
			response = e.read()
		return response

	def delete_folder (self, path):
		payload = {"path":path}
		request = urllib2.Request(self.api_url + "/folders/?access_token=" + self.access_token, urllib.urlencode(payload))
		request.get_method = lambda: 'DELETE'
		response = json.load(urllib2.urlopen(request))
		return response

	# File API Methods
        def download_file_part (self, download_url, offset, size, total_size):
                #print "Downloading file from URL: " + download_url
		print "Threads: "+str(threading.active_count())+" "+str(threading.current_thread().ident)
		rangeHeader = None
		if ((offset + size) > total_size):
			rangeHeader = {'Range':'bytes='+str(offset)+'-'+str(total_size)}
		else:
			rangeHeader = {'Range':'bytes='+str(offset)+'-'+str(offset+(size-1))}
		pprint.pprint (rangeHeader)
		return_data = ""
		r3 = None
		threadId = threading.current_thread().ident
	        try:   
			print "download_file_part create request"
			r3 = self.httpsPool.request("GET", download_url,headers=rangeHeader,retries=self.retry)
			print "download_file_part ("+str(threadId)+") get response, connection used: "+str(self.httpsPool.num_connections)
			print "download_file_part get data "+str(size)
	        except Exception as e:
			print "Exception (download_file_part): "+str(type(e))+" "+str(e)
			return
                return r3.data


        def download_file_url (self, file_id, path, file_name, file_size):
                #file_url = self.api_url + "/files/"+file_id+"/"+ urllib.quote_plus(file_name) +"?access_token=" + self.access_token + "&path=/" + path
		file_url = self.api_path + "/files/"+file_id+"/"+ urllib.quote_plus(file_name) +"?access_token=" + self.access_token + "&path=/" + path
                print "File URL: " + file_url
		return file_url

	def upload_file (self, path, file_name, file_size):
		return
	# def rename_folder (self, path, new_filename, exists = "rename"):
	# 	# Encode path
	# 	if(path != ""):
	# 		path = base64.b64encode(path)
	# 	else:
	# 		return "You must specify a file to rename."
		
	# 	payload = { "operation" = "" }
	# 	data = urllib.urlencode(payload)

	# 	request = urllib2.Request(Bitcasa.api_url + "/folders/" + path + "?access_token=" + self.access_token, data)
	# 	response = json.load(urllib2.urlopen(request))
	# 	return response;
