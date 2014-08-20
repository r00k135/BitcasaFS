# Bitcasa Python Class (Unofficial)
# Original by: Michael Thomas, 2013 
# Updated by Chris Elleman (@chris_elleman), 2014
# TODO #
########
# Update buffer to account for multiple processes access
# Remove the need to do a find / to cache the filesystem in linux
# De-couple the readAhead buffering from the read operation/download_file_part functions

# Import Section
import urllib2, urllib, urllib3, httplib, socket, json, base64
import sys, os, io
import pprint, time, datetime
from urlparse import urlparse
import mutex, threading, workerpool
from collections import namedtuple
from itertools import count

class DownloadJob(workerpool.Job):
	def __init__(self, download_url, rangeHeader, bcParent, size):
		# should execute within a mutex
		self.download_url = download_url
		self.rangeHeader = rangeHeader
		self.size = size
		self.bcParent = bcParent
		if self.rangeHeader != None:
			newBuf = namedtuple('newBuf', 'data, complete, size')
			if str(self.rangeHeader) not in self.bcParent.aheadBuffer:
				print "download_file_part create buffer "+str(self.rangeHeader)+" size:"+str(self.size)
				self.bcParent.aheadBuffer[self.download_url+str(self.rangeHeader)] = newBuf
				self.bcParent.aheadBuffer[self.download_url+str(self.rangeHeader)].complete = 0
				self.bcParent.aheadBuffer[self.download_url+str(self.rangeHeader)].size = self.size
	def run(self):
		#print "MyJob Downloading file from URL: " + self.download_url
		threadId = threading.current_thread().ident
		print "Threads: "+str(threading.active_count())+" "+str(threadId)
		downLoop = 1
		while downLoop != 0:
			r3 = None
			try:
				print "download_file_part create request"
				r3 = self.bcParent.httpsPool.request("GET", self.download_url,headers=self.rangeHeader,retries=self.bcParent.retry)
				print "download_file_part get data "+str(self.rangeHeader)+" connection used: "+str(self.bcParent.httpsPool.num_connections)
				downLoop = 0
			except Exception as e:
				print "Exception (DownloadJob): "+str(type(e))+" "+str(e)
				downLoop += 1
				if downLoop > 4:
					print "Error: DownloadJob downLoop too high, can't download range:"+str(self.rangeHeader)
					return
			self.bcParent.aheadBuffer_mutex.acquire()
			if self.bcParent.aheadBuffer[self.download_url+str(self.rangeHeader)].complete == 0:
				print "download_file_part update buffer "+str(self.rangeHeader)+" size:"+str(self.size)
				self.bcParent.aheadBuffer[self.download_url+str(self.rangeHeader)].data = r3.data
				self.bcParent.aheadBuffer[self.download_url+str(self.rangeHeader)].complete = 1
			self.bcParent.aheadBuffer_mutex.release()


class DownloadChunk(workerpool.Job):
	def __init__(self, download_url, rangeHeaderKeys, bcParent, chunk_size, buffer_size, start_byte, end_byte):
		# should execute within a mutex
		self.download_url = download_url
		self.rangeHeaderKeys = rangeHeaderKeys
		self.bcParent = bcParent
		self.chunk_size = chunk_size
		self.buffer_size = buffer_size
		self.start_byte = start_byte
		self.end_byte = end_byte
	def run(self):
		#print "DownloadChunk Downloading file from URL: " + self.download_url
		threadId = threading.current_thread().ident
		print "Threads: "+str(threading.active_count())+" "+str(threadId)
		downLoop = 1
		while downLoop != 0:
			r3 = None
			requestHeader = None
			tracked_data = None
			try:
				requestHeader = {'Range':'bytes='+str(self.start_byte)+'-'+str(self.end_byte)}
				print "DownloadChunk create range request: "+str(requestHeader)
				#r3 = self.bcParent.httpsPool.urlopen("GET", self.download_url,headers=requestHeader,retries=self.bcParent.retry)
				r3 = self.bcParent.httpsPool.urlopen("GET", self.download_url, headers=requestHeader,retries=self.bcParent.retry,preload_content=False)
				print str(r3)+" buffer_size:"+str(self.buffer_size)
				buffered_data = r3.stream(self.chunk_size)
				print "start reading rangeHeaderKeys"
				headerCnt = 0
				for tracked_data in buffered_data:
					if headerCnt < len(self.rangeHeaderKeys):
						headerH = self.rangeHeaderKeys[headerCnt]
						if self.download_url+str(headerH) in self.bcParent.aheadBuffer:
							print "DownloadChunk rangeHeaderKeys Loop "+str(headerH)
							# Save data in buffer
							self.bcParent.aheadBuffer_mutex.acquire()
							print "DownloadChunk rangeHeaderKeys Loop - acquire "+str(headerH)
							if self.bcParent.aheadBuffer[self.download_url+str(headerH)].complete == 0:
								print "download_file_part update buffer "+str(headerH)+" size:"+str(self.chunk_size)
								self.bcParent.aheadBuffer[self.download_url+str(headerH)].data = tracked_data
								self.bcParent.aheadBuffer[self.download_url+str(headerH)].complete = 1
							self.bcParent.aheadBuffer_mutex.release()
							print "DownloadChunk rangeHeaderKeys Loop - release "+str(headerH)
						else:
							print "Error (download_file_part): index not found: "+str(headerH)
						headerCnt += 1
				r3.release_conn()
				downLoop = 0
			except Exception as e:
				print "Exception (DownloadChunk): "+str(type(e))+" "+str(e)
				downLoop += 1
				if downLoop > 4:
					print "Error: DownloadJob downLoop too high, can't download range:"+str(self.start_byte)+"-"+str(self.end_byte)
					downLoop = 0
		print "Ending Worker: "+str(requestHeader)
		return

# Bitcasa Class
class Bitcasa:
	httpsPool = None
	aheadBuffer = dict()
	aheadBuffer_mutex = threading.Lock()
	bufferSizeCnt = dict()
	bufferSizeCnt_mutex = threading.Lock()
	NUM_WORKERS = 5
	NUM_SOCKETS = NUM_WORKERS+2
	NUM_CHUNKS = 10
	download_pause = 0
	retry = urllib3.util.Retry(read=3, backoff_factor=2)

	# Start Client & Load Config
	def __init__ (self, config_path):
		# Config file
		self.config_path = config_path
		try:
			with open(self.config_path, 'r') as config_file:
				self.config = json.load(config_file)
		except:
			sys.exit("Exception (Bitcasa:init): Could not find configuration file: "+self.config_path)
		# Set configuation variables
		self.api_url = self.config['api_url'].encode('utf-8')
		self.client_id = self.config['client_id'].encode('utf-8')
		self.secret = self.config['secret'].encode('utf-8')
		self.redirect_url = self.config['redirect_url'].encode('utf-8')
		self.auth_token = self.config['auth_token'].encode('utf-8')
		self.access_token = self.config['access_token'].encode('utf-8')
		# Adding support for File Cache - ToDo remove requirement
		self.cache_dir = self.config['cache_dir'].encode('utf-8')
		if(self.cache_dir == None):
			sys.exit("Exception (Bitcasa:init): Could not find cache_dir config variable: ")
		# Now Make sure it exists
		if not os.path.exists(self.cache_dir):
			sys.exit("Exception (Bitcasa:init): cache_dir doesn't exist: "+self.cache_dir)
		# See if we need our tokens
		if(self.auth_token == "") or (self.access_token == ""):
			return self.authenticate()
		# Initiate Connection
		self.api_host = urlparse(self.api_url).hostname
		self.api_path = urlparse(self.api_url).path
		#urllib3.connection.HTTPSConnection.default_socket_options + [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),]
		#urllib3.connection.HTTPConnection.default_socket_options + [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),]
		# Create Connection Pool
		self.httpsPool = urllib3.HTTPSConnectionPool(self.api_host, maxsize=self.NUM_SOCKETS, timeout=urllib3.Timeout(connect=2.0, read=5.0))
		return None

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
			r2 = self.httpsPool.request("GET", list_folder_url,retries=self.retry)
			print "list_folder: "+str(r2.status)
			raw_response = r2.data
			r2.release_conn()
			print "list_folder.raw_response = "+raw_response
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
		rangeHeader = None
		return_data = None
		start_byte = offset
		end_byte = offset
		if (offset + size) > total_size:
			rangeHeader = {'Range':'bytes='+str(offset)+'-'+str(total_size)}
			end_byte = total_size
		else:
			rangeHeader = {'Range':'bytes='+str(offset)+'-'+str(offset+(size-1))}
			end_byte = offset+(size-1)
		print "download_file_part RangeHeader top check: "+str(rangeHeader)
		# TRACK BLOCK SIZE
		self.bufferSizeCnt_mutex.acquire()
		if size in self.bufferSizeCnt:
			self.bufferSizeCnt[size] += 1
		else:
			self.bufferSizeCnt[size] = 1
		self.bufferSizeCnt_mutex.release()
		# CHECK FOR RESULT BEING PRE-BUFFERED
		endLoop = 1
		while endLoop != 0:
			self.aheadBuffer_mutex.acquire()
			if download_url+str(rangeHeader) in self.aheadBuffer:
				# Wait to download the current requested block
				print "download_file_part download found in buffer"+str(rangeHeader)
				self.aheadBuffer_mutex.release()
				endLoop = 0
				sleepCnt = 0
				while self.aheadBuffer[download_url+str(rangeHeader)].complete != 1:
					print "download_file_part RangeHeader sleep: "+str(rangeHeader)
					time.sleep(1)
					sleepCnt += 1
					if sleepCnt > 600:
						print "Error (download_file_part) download wait timeout: "+str(rangeHeader)
						return
				self.aheadBuffer_mutex.acquire()
				return_data = self.aheadBuffer[download_url+str(rangeHeader)].data
				self.aheadBuffer.pop(download_url+str(rangeHeader), None)
				self.aheadBuffer_mutex.release()
			else:
				self.aheadBuffer_mutex.release()
				pool = workerpool.WorkerPool(size=self.NUM_WORKERS)
				if ((offset + size) > total_size) or (self.bufferSizeCnt[size] < 3):
					# ADD SINGLE GET
					self.aheadBuffer_mutex.acquire()
					if (download_url+str(rangeHeader) not in self.aheadBuffer) and (rangeHeader != None):
						self.createBuffer(download_url, rangeHeader, size)
						print "download_file_part add range header: "+str(rangeHeader)+" Singleton size:"+str(size)
						job = DownloadChunk(download_url, [rangeHeader], self, size, size, start_byte, end_byte)
						pool.put(job)
					self.aheadBuffer_mutex.release()
					pool.shutdown()
					print "download_file_part single wait send shutdown: "+str(rangeHeader)+" Singleton size:"+str(size)
					pool.wait()
					print "download_file_part single wait finished: "+str(rangeHeader)+" Singleton size:"+str(size)
				else:
					# ADD MULTIPLE RANGES
					if self.download_pause == 0:
						self.download_pause = 1	
						print "download_file_part Multiple add job: "+str(rangeHeader)+" size:"+str(size)+" count:"+str(self.bufferSizeCnt[size])
						# Append already calculated
						new_offset = offset
						for work in range(self.NUM_WORKERS):
							if new_offset < total_size:
								start_byte = new_offset
								ranges = []
								max_chunks = int((total_size - new_offset) / size)
								if max_chunks > self.NUM_CHUNKS:
									max_chunks = self.NUM_CHUNKS
								end_byte = start_byte + ((max_chunks * size)-1)
								print "download_file_part Multiple add job: "+str(rangeHeader)+" max_chunks:"+str(max_chunks)+" start_byte:"+str(start_byte)+" end_byte:"+str(end_byte)
								self.aheadBuffer_mutex.acquire()
								for chunk in range(max_chunks):
									new_rangeHeader = {'Range':'bytes='+str(new_offset)+'-'+str(new_offset+(size-1))}
									if (download_url+str(new_rangeHeader) not in self.aheadBuffer) and (new_rangeHeader != None):
										self.createBuffer(download_url, new_rangeHeader, size)
									ranges.append (new_rangeHeader)
									new_offset = new_offset+size
								self.aheadBuffer_mutex.release()
								print "download_file_part start worker:"+str(work)+", ranges: "+str(ranges)
								job = DownloadChunk(download_url, ranges, self, size, size, start_byte, end_byte)
								pool.put(job)
						endLoop += 1
						if endLoop >  4:
							print "Error: download_file_part max endLoop exceeded: "+str(endLoop)
						self.download_pause = 0
					else:
						time.sleep(0.5)
						print "download_file_part Multiple add job - download pause "+str(rangeHeader)+" size:"+str(size)+" count:"+str(self.bufferSizeCnt[size])
		return return_data


	def createBuffer (self, download_url, rangeHeader, chunk_size):
		newBuf = namedtuple('newBuf', 'data, complete, chunk_size')
		newBuf.complete = 0
		newBuf.chunk_size = chunk_size
		if download_url+str(rangeHeader) not in self.aheadBuffer:
			print "Bitcasa:createBuffer create buffer:"+str(rangeHeader)+" size:"+str(chunk_size)
			self.aheadBuffer[download_url+str(rangeHeader)] = newBuf
		else:
			print "Error (Bitcasa:createBuffer) create buffer:"+str(rangeHeader)+" size:"+str(chunk_size)+" already exists"


	def download_file_url (self, file_id, path, file_name, file_size):
		file_url = self.api_path + "/files/"+file_id+"/"+ urllib.quote_plus(file_name) +"?access_token=" + self.access_token + "&path=/" + path
		print "File URL: " + file_url
		return file_url


	def upload_file (self, path, file_name, file_size):
		return
