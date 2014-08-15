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
import urllib2, urllib
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
		data = file.read(self, size)
		self._callback(self._total, len(data), *self._args)
		return data
# e.g.
# path = 'large_file.txt'
# progress = Progress()
# stream = file_with_callback(path, 'rb', progress.update, path)
# req = urllib2.Request(url, stream)
# res = urllib2.urlopen(req)

# Bitcasa Class
class Bitcasa:
	httpsConns = dict()
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
		return None

	def assignConnection (self, threadId):
		if threadId in self.httpsConns:
			try:
				print "peerName: ("+str(threadId)+") "+str(self.httpsConns[threadId].https_conn.sock.getpeername())
				self.httpsConns[threadId].cnt += 1
			except socket.error, (value,message):
				try:
					print  "peerName: ("+str(threadId)+") reconnecting socket val:"+str(value)+" msg:"+str(message)
					self.httpsConns[threadId].mutex.acquire()
					#self.httpsConns[threadId].https_conn.close()
					self.httpsConns[threadId].https_conn = httplib.HTTPSConnection(self.api_host, timeout=20)
	                                self.httpsConns[threadId].https_conn.connect()
	                                self.httpsConns[threadId].https_conn.sock.settimeout(20)
					self.httpsConns[threadId].mutex.release()
				except socket.error, (value1,message1):
					print "reconnect error val:"+str(value1)+" msg:"+str(message1)
		else:
			try:
				newConn = namedtuple('newConn', 'https_conn, mutex, cnt, last_access')
				self.httpsConns[threadId] = newConn
				self.httpsConns[threadId].https_conn = httplib.HTTPSConnection(self.api_host, timeout=20)
				self.httpsConns[threadId].https_conn.connect()
				self.httpsConns[threadId].https_conn.sock.settimeout(20)
				self.httpsConns[threadId].mutex = threading.Lock()
				self.httpsConns[threadId].cnt = 0
				self.httpsConns[threadId].mutex.acquire()
				self.httpsConns[threadId].https_conn.connect()
				self.httpsConns[threadId].https_conn.request("GET", self.api_path, headers={"Connection":" keep-alive"})
				r1 = self.httpsConns[threadId].https_conn.getresponse()
				print "assignConnection ("+str(threadId)+") status: "+str(r1.status)
				r1.read()
			except httplib.HTTPException, e:
				print "Exception (Bitcasa:assignConnection:newConnect - "+str(threadId)
			self.httpsConns[threadId].mutex.release()
		self.httpsConns[threadId].last_access = time.time()
		return self.httpsConns[threadId]

	def recycleConnection (self, threadId):
		if threadId not in self.httpsConns:
			print "recycleConnection:assignConnection ("+str(threadId)+")"
			self.assignConnection (threadId)
		else:
			print "recycleConnection:recycle ("+str(threadId)+")"
			self.httpsConns[threadId].mutex.acquire()
			self.httpsConns[threadId].https_conn.close()
			self.httpsConns[threadId].https_conn = httplib.HTTPSConnection(self.api_host, timeout=20)
			self.httpsConns[threadId].https_conn.connect()
			self.httpsConns[threadId].https_conn.sock.settimeout(20)
			self.httpsConns[threadId].mutex.release()
			print "recycleConnection:recycle ("+str(threadId)+") connected"
			
		
	def HTTPSdisconnect (self):
		self.mutex.acquire()
		try:
			self.https_conn.close()
		except HTTPException, e:
			print "Exception (Bitcasa:HTTPSdisconnect): "+str(e)
		self.mutex.release()

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
		#request = urllib2.Request(self.api_url + "/folders" + path + "?access_token=" + self.access_token)
		print "Threads: "+str(threading.active_count())+" "+str(threading.current_thread().ident)
		conn = self.assignConnection (threading.current_thread().ident)
		conn.mutex.acquire()
		r2 = None
		response = None
		try:
			list_folder_url = self.api_path + "/folders" + path + "?access_token=" + self.access_token
			print "list_folder.list_folder_url = "+list_folder_url
			conn.https_conn.request("GET", list_folder_url)
			r2 = conn.https_conn.getresponse()
			print "list_folder: "+str(r2.status)+" "+r2.reason
			raw_response = r2.read()
			print "list_folder.raw_response = "+raw_response
			response = json.loads(raw_response)
		except Exception, e:
			print "Exception (list_folder) "+str(type(e))+" "+str(e)
		conn.mutex.release()
		if(response['result'] == None):
			return response
		else:
			return response['result']['items']

	def add_folder (self, path, folder_name):
		payload = {"folder_name":folder_name}
		request = urllib2.Request(self.api_url + "/folders/" + path + "?access_token=" + self.access_token, urllib.urlencode(payload))
		try:
			response = json.load(urllib2.urlopen(request))
		except urllib2.HTTPError, e:
			response = e.read()
		return response

	def delete_folder (self, path):
		payload = {"path":path}
		request = urllib2.Request(self.api_url + "/folders/?access_token=" + self.access_token, urllib.urlencode(payload))
		request.get_method = lambda: 'DELETE'
		response = json.load(urllib2.urlopen(request))
		return response

	# File API Methods
	def download_file (self, file_id, path, file_name, file_size):
		local_file = self.cache_dir + "/" + file_name
		f = open(local_file, 'wb')
		file_url = self.api_url + "/files/"+file_id+"/"+ urllib.quote_plus(file_name) +"?access_token=" + self.access_token + "&path=/" + path
		print "Downloading file from URL: " + file_url
		req = urllib2.Request(file_url)
		try:
			u = urllib2.urlopen(req)
		except urllib2.URLError as e:
			print e.reason
			return ""
		print "Downloading: %s Bytes: %s" % (file_name, file_size)
		file_size_dl = 0
		block_sz = 8192
		
		while True:
			buffer = u.read(block_sz)
			if not buffer:
				break
			file_size_dl += len(buffer)
			f.write(buffer)
			status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
			status = status + chr(8)*(len(status)+1)
			print status
		print "Closing new File"
		f.close()
		return local_file

        def download_file_part (self, download_url, offset, size, total_size):
                print "Downloading file from URL: " + download_url
		print "Threads: "+str(threading.active_count())+" "+str(threading.current_thread().ident)
		rangeHeader = None
		if ((offset + size) > total_size):
			rangeHeader = {'Range':'bytes='+str(offset)+'-'+str(total_size)}
		else:
			rangeHeader = {'Range':'bytes='+str(offset)+'-'+str(offset+(size-1))}
		pprint.pprint (rangeHeader)
		return_data = ""
		r3 = None
		endLoop = 1
		conn = None
		threadId = threading.current_thread().ident
		while endLoop != 0:
	                try:   
				conn = self.assignConnection (threading.current_thread().ident)
				print "download_file_part getmutex"
				conn.mutex.acquire()
				print "download_file_part create request"
				conn.https_conn.request("GET", download_url,headers=rangeHeader)
				print "download_file_part ("+str(threadId)+") get response, connection used: "+str(conn.cnt)
				r3 = conn.https_conn.getresponse()
				print "download_file_part get data "+str(size)
				return_data = r3.read(size)
				additional_data = r3.read()
				print "download_file_part.additional_data "+additional_data+" "+str(len(additional_data))
				conn.mutex.release()
				endLoop = 0
	                except Exception as e:
				if conn != None:
					conn.mutex.release()
				print "Exception (download_file_part): "+str(type(e))+" "+str(e)+" endLoop:"+str(endLoop)
				self.recycleConnection(threading.current_thread().ident)
				endLoop += 1
				if endLoop > 10:
					print "Exception (download_file_part): timeout"
					return
                return return_data


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
