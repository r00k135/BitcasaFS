# Bitcasa Python Class (Unofficial)
# Original by: Michael Thomas, 2013 
# Updated by Chris Elleman (@chris_elleman), 2014
# TODO #
########
# Change all functions so that when they output, they all use a standard notation to show the ThreadId
# Remove the need to do a "find /" to cache the filesystem in linux
# De-couple the readAhead buffering from the read operation/download_file_part functions

# Import Section
import urllib2, urllib, urllib3, httplib, socket, json, base64
import sys, os, io, logging
import pprint, time, datetime
from urlparse import urlparse
import mutex, threading, workerpool, thread, traceback
from collections import namedtuple
from itertools import count
import BaseHTTPServer


class bcfsHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
	def do_HEAD(self):
		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.end_headers()
	def do_GET(self):
		"""Check API Version"""
		APIcheck = "/bcfsapi20140823/"
		APIpath = self.path.replace(APIcheck, "", 1)
		APImethod = APIpath.split('/')[0]
		if (self.path.find(APIcheck) == 0) and (APImethod != ""):
			if APImethod == "bcfs_thread_status":
				self.send_response(200)
				self.send_header("Content-type", "application/json")
				self.end_headers()
				self.wfile.write('{\r\n"APImethod":"bcfs_thread_status",\r\n')
				self.wfile.write('"time":"%s",\r\n' % str(time.time()))
				self.wfile.write('"threads": [\r\n\t')
				id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
				code = []
				firstThread = 1
				for threadId, stack in sys._current_frames().items():
					if firstThread == 1:
						code.append('{"threadName": "%s", "threadId": "%d",' % (id2name.get(threadId,""), threadId))
						firstThread = 0
					else:
						code.append(',{"threadName": "%s", "threadId": "%d",' % (id2name.get(threadId,""), threadId))
					code.append('\t"trace": [')
					lastline = ""
					firstline = 1
					for filename, lineno, name, line in traceback.extract_stack(stack):
						line = line.strip()
						line = line.replace('"', '\\"')
						if firstline == 1:
							code.append('\t\t{"filename": "%s", "line": %d, "function": "%s", "line": "%s"}' % (filename, lineno, name, line.strip()))
							firstline = 0
						else:
							code.append('\t\t,{"filename": "%s", "line": %d, "function": "%s", "line": "%s"}' % (filename, lineno, name, line.strip()))
						lastline=line.strip()
					code.append('\t],')
					code.append('\t"line": "%s"' % lastline)
					code.append ('}')
				self.wfile.write("\r\n\t".join(code))
				self.wfile.write('\r\n\t]\r\n')
				self.wfile.write('}\r\n')
			elif APImethod == "bcfs_buffer_sizes":
				self.send_response(200)
				self.send_header("Content-type", "application/json")
				self.end_headers()
				self.wfile.write('{\r\n"APImethod":"bcfs_buffer_sizes",\r\n')
				self.wfile.write('"time":"%s",\r\n' % str(time.time()))
				self.wfile.write('"buffers": [\r\n')
				self.wfile.write('\t{"aheadBufferLen": "%s"},\r\n' % str(len(self.server.bitcasa.aheadBuffer)))
				self.wfile.write('\t{"aheadBufferLockedBy": "%s"},\r\n' % str(self.server.bitcasa.aheadBuffer_lockedByThread))
				self.wfile.write('\t{"bufferSizeCntLen": "%s"},\r\n' % str(len(self.server.bitcasa.bufferSizeCnt)))
				self.wfile.write('\t{"active_connections": "%s"}\r\n' % str(self.server.bitcasa.active_connections))
				self.wfile.write('\t]\r\n')
				self.wfile.write('}\r\n')
			else:
				self.send_response(200)
				self.send_header("Content-type", "text/html")
				self.end_headers()
				self.wfile.write("<html><head><title>APImethods</title></head>\r\n")
				self.wfile.write("<p>You accessed path: "+self.path+" which is not recognised, try these instead:</p>\r\n")
				self.wfile.write("<p>"+APIpath+"/bcfs_thread_status - show the status of all threads, returns JSON</p>\r\n")
				self.wfile.write("</body></html>\r\n")
		else:
			# Serve from docroot folder (html)
			docrootPath = self.path.replace("/", "html/", 1)
			docrootPath = docrootPath.replace ("..", "") # bit of extra security to stop breaking out of the docroot
			# check for replace for default index
			if docrootPath == "html/":
				docrootPath = "html/index.html"
			# set mimetype based on file extension, otherwise default to plain text, references taken from: http://www.sitepoint.com/web-foundations/mime-types-complete-list/
			mimetype='text/plain'
			if docrootPath.endswith(".html"):
				mimetype='text/html'
			if docrootPath.endswith(".ico"):
				mimetype='image/x-icon'
			if docrootPath.endswith(".css"):
				mimetype='text/css'
			if docrootPath.endswith(".js"):
				mimetype='application/javascript'
			if docrootPath.endswith(".json"):
				mimetype='application/javascript'
			if docrootPath.endswith(".xml"):
				mimetype='application/xml'
			if docrootPath.endswith(".jpg"):
				mimetype='image/jpg'
			if docrootPath.endswith(".gif"):
				mimetype='image/gif'
			found = 0
			try:
				fh = open (docrootPath, "r")
				found = 1
				self.send_response(200)
				self.send_header("Content-type", "%s" % mimetype)
				self.end_headers()
				for line in fh:
					self.wfile.write(line)
				fh.close()
			except Exception as e:
				# file can't be open
				pass
			if found == 0:
				self.send_response(404)
				self.send_header("Content-type", "text/html")
				self.end_headers()
				self.wfile.write("<html><head><title>Error: 404 not found</title></head>\r\n")
				self.wfile.write("<p>Error: API version mismatch or HTML not found "+self.path+"</p>\r\n")
				self.wfile.write("</body></html>\r\n")


class bcfsHTTPthread(threading.Thread):
	def __init__(self, http_ip, http_port,bcfslog,bitcasa):
		threading.Thread.__init__(self)
		self.name = "bcfsHTTPthread"
		self.server_class = BaseHTTPServer.HTTPServer
		self.http_ip = http_ip
		self.http_port = http_port
		self.bcfslog = bcfslog
		self.bitcasa = bitcasa
		self.bcfslog.debug("Initialise thread for HTTP server")
		self.httpd = self.server_class((self.http_ip, int(self.http_port)), bcfsHTTPHandler)
		self.httpd.bitcasa = bitcasa

	def run(self):
		try:
			self.bcfslog.debug("Listen on "+self.http_ip+" port:"+self.http_port)
			self.httpd.serve_forever()
		except Exception as e:
			self.bcfslog.debug("Exception on HTTP server "+str(e))

   	def stop(self):
   		self.bcfslog.debug("stopping http server")
   		self.httpd.server_close()


class DownloadChunk(workerpool.Job):
	def __init__(self, download_url, rangeHeaderKeys, bcParent, chunk_size, buffer_size, start_byte, end_byte, client_pid, bcfslog):
		# should execute within a mutex
		self.download_url = download_url
		self.rangeHeaderKeys = rangeHeaderKeys
		self.bcParent = bcParent
		self.chunk_size = chunk_size
		self.buffer_size = buffer_size
		self.start_byte = start_byte
		self.end_byte = end_byte
		self.client_pid = client_pid
		self.bcfslog = bcfslog

	def run(self):
		self.bcfslog.debug("Starting Download Worker")
		downLoop = 1
		while downLoop != 0:
			r3 = None
			requestHeader = None
			tracked_data = None
			try:
				requestHeader = {'Range':'bytes='+str(self.start_byte)+'-'+str(self.end_byte)}
				self.bcfslog.debug("create range request: "+str(requestHeader)+" buffer_size: "+str(self.buffer_size))
				self.bcfslog.debug("requesting connection from https pool: "+str(requestHeader)+" buffer_size: "+str(self.buffer_size))
				tstart = datetime.datetime.now()
				r3 = self.bcParent.httpsPool.urlopen("GET", self.download_url, assert_same_host=True, headers=requestHeader,retries=self.bcParent.retry,preload_content=False)
				self.bcParent.active_connections += 1
				tdelta = datetime.datetime.now() - tstart
				self.bcfslog.debug("got connection from https pool, took ("+str(int(tdelta.total_seconds()*1000))+"ms), now create stream: "+str(requestHeader)+" buffer_size: "+str(self.buffer_size))
				buffered_data = r3.stream(self.chunk_size)
				self.bcfslog.debug("stream created, start reading rangeHeaderKeys"+str(requestHeader)+" buffer_size: "+str(self.buffer_size))
				headerCnt = 0
				headerLen = len(self.rangeHeaderKeys)
				tstart = datetime.datetime.now()
				for tracked_data in buffered_data:
					if headerCnt < headerLen:
						headerH = self.rangeHeaderKeys[headerCnt]
						if self.client_pid+":"+self.download_url+str(headerH) in self.bcParent.aheadBuffer:
							self.bcfslog.debug("rangeHeaderKeys Loop "+str(headerH)+" save data in buffer")
							self.bcParent.aheadBuffer_mutex.acquire()
							self.bcParent.aheadBuffer_lockedByThread = threading.current_thread().ident
							self.bcfslog.debug("acquire aheadBuffer_mutex")
							if self.bcParent.aheadBuffer[self.client_pid+":"+self.download_url+str(headerH)].complete == 0:
								self.bcfslog.debug("update aheadBuffer item with data "+str(headerH)+" size:"+str(self.chunk_size))
								self.bcParent.aheadBuffer[self.client_pid+":"+self.download_url+str(headerH)].data = tracked_data
								self.bcParent.aheadBuffer[self.client_pid+":"+self.download_url+str(headerH)].complete = 1
								self.bcParent.aheadBuffer[self.client_pid+":"+self.download_url+str(headerH)].last_accessed = time.time()
							else:
								self.bcfslog.debug("aheadBuffer item already completed" +str(headerH))
							self.bcParent.aheadBuffer_lockedByThread = 0
							self.bcfslog.debug("realease aheadBuffer_mutex")
							self.bcParent.aheadBuffer_mutex.release()
						else:
							self.bcfslog.debug("Error (download_file_part): index not found: "+str(headerH))
						headerCnt += 1
					else:
						self.bcfslog.debug("headerCnt ("+str(headerCnt)+" > headerLen ("+str(headerLen)+")")
				tdelta = datetime.datetime.now() - tstart
				r3.flush()
				r3.release_conn()
				self.bcParent.active_connections -= 1
				self.bcfslog.debug("connection released back to the pool, download time ("+str(int(tdelta.total_seconds()*1000))+"ms): "+str(requestHeader)+" buffer_size: "+str(self.buffer_size))
				downLoop = 0
			except Exception as e:
				self.bcfslog.error("Exception (DownloadChunk): "+str(type(e))+" "+str(e))
				try:
					self.bcParent.aheadBuffer_mutex.release()
					self.bcfslog.debug("Exception: force release aheadBuffer_mutex"+str(requestHeader))
				except Exception as e:
					self.bcfslog.info("Exception: force release aheadBuffer_mutex - mutex wasn't locked: "+str(requestHeader))
					pass
				downLoop += 1
				if downLoop > 4:
					self.bcfslog.error("Error: DownloadJob downLoop too high, can't download range:"+str(self.start_byte)+"-"+str(self.end_byte))
					downLoop = 0
		self.bcfslog.debug("Ending Download Worker: "+str(requestHeader))
		return

# Bitcasa Class
class Bitcasa:
	httpsPool = None
	aheadBuffer = dict()
	aheadBuffer_mutex = threading.Lock()
	aheadBuffer_lockedByThread = 0
	bufferSizeCnt = dict()
	bufferSizeCnt_mutex = threading.Lock()
	NUM_WORKERS = 4
	NUM_SOCKETS = NUM_WORKERS+2
	NUM_CHUNKS = 10  # must be an even number
	download_pause = 0
	download_pause_mutex = threading.Lock()
	retry = urllib3.util.Retry(read=10, backoff_factor=2)
	timeout = urllib3.Timeout(connect=5.0, read=10.0)
	pool = workerpool.WorkerPool(size=NUM_WORKERS)
	httpd_thread = None
	active_connections = 0
	bcfslog = None

	# Start Client & Load Config
	def __init__ (self, config_path, bcfslog):
		# Config file
		self.config_path = config_path
		# Logger
		self.bcfslog = bcfslog
		self.bcfslog.debug("Initialise filesystem")

		try:
			with open(self.config_path, 'r') as config_file:
				self.config = json.load(config_file)
		except:
			self.bcfslog.error("Exception Could not find configuration file: "+self.config_path)
			sys.exit("Could not find configuration file: "+self.config_path)
		# Set configuation variables
		self.api_url = self.config['api_url'].encode('utf-8')
		self.client_id = self.config['client_id'].encode('utf-8')
		self.secret = self.config['secret'].encode('utf-8')
		self.redirect_url = self.config['redirect_url'].encode('utf-8')
		self.auth_token = self.config['auth_token'].encode('utf-8')
		self.access_token = self.config['access_token'].encode('utf-8')
		# See if we need our tokens
		if(self.auth_token == "") or (self.access_token == ""):
			return self.authenticate()
		# Start HTTP Server
		self.httpd_thread = bcfsHTTPthread (self.config['http_ip'].encode('utf-8'), self.config['http_port'].encode('utf-8'), bcfslog, self)
		self.httpd_thread.start()
		# Initiate Connections
		self.api_host = urlparse(self.api_url).hostname
		self.api_path = urlparse(self.api_url).path
		#urllib3.connection.HTTPSConnection.default_socket_options + [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
		#urllib3.connection.HTTPConnection.default_socket_options + [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
		# Create Connection Pool
		self.bcfslog.debug("create connection pool")
		self.httpsPool = urllib3.HTTPSConnectionPool(self.api_host, maxsize=self.NUM_SOCKETS, timeout=self.timeout)

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
		self.bcfslog.debug("starting")
		r2 = None
		response = None
		try:
			list_folder_url = self.api_path + "/folders" + path + "?access_token=" + self.access_token
			self.bcfslog.debug("list_folder_url = "+list_folder_url)
			r2 = self.httpsPool.request("GET", list_folder_url,retries=self.retry)
			self.active_connections += 1
			self.bcfslog.debug("connection status: "+str(r2.status))
			raw_response = r2.data
			r2.release_conn()
			self.active_connections -= 1
			self.bcfslog.debug("raw_response = "+raw_response)
			response = json.loads(raw_response)
		except Exception, e:
			self.bcfslog.error("Exception: "+str(e))
			return {}
		if(response['result'] == None):
			self.bcfslog.error("ending - no response")
			return response
		else:
			print self.bcfslog.debug("ending - with data")
			return response['result']['items']

	#def add_folder (self, path, folder_name):
	#	payload = {"folder_name":folder_name}
	#	request = urllib2.Request(self.api_url + "/folders/" + path + "?access_token=" + self.access_token, urllib.urlencode(payload))
	#	try:
	#		response = json.load(urllib2.urlopen(request))
	#	except httplib2.HttpLib2Error, e:
	#		response = e.read()
	#	return response

	#def delete_folder (self, path):
	#	payload = {"path":path}
	#	request = urllib2.Request(self.api_url + "/folders/?access_token=" + self.access_token, urllib.urlencode(payload))
	#	request.get_method = lambda: 'DELETE'
	#	response = json.load(urllib2.urlopen(request))
	#	return response

	# File API Methods
	def download_file_part (self, download_url, offset, size, total_size, client_pid):
		self.bcfslog.debug("starting")
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
		self.bcfslog.debug("RangeHeader top check: "+str(rangeHeader))
		# TRACK BLOCK SIZE
		self.bufferSizeCnt_mutex.acquire()
		if client_pid+":"+download_url+":"+str(size) in self.bufferSizeCnt:
			self.bufferSizeCnt[client_pid+":"+download_url+":"+str(size)] += 1
		else:
			self.bufferSizeCnt[client_pid+":"+download_url+":"+str(size)] = 1
		self.bufferSizeCnt_mutex.release()
		# CHECK FOR RESULT BEING PRE-BUFFERED
		endLoop = 1
		while endLoop != 0:
			self.aheadBuffer_mutex.acquire()
			self.aheadBuffer_lockedByThread = threading.current_thread().ident
			self.bcfslog.debug("acquire aheadBuffer_mutex")
			if client_pid+":"+download_url+str(rangeHeader) in self.aheadBuffer:
				# Wait to download the current requested block
				self.bcfslog.debug("download found in buffer"+str(rangeHeader))
				self.aheadBuffer_lockedByThread = 0
				self.bcfslog.debug("release aheadBuffer_mutex")
				self.aheadBuffer_mutex.release()
				sleepCnt = 0
				while self.aheadBuffer[client_pid+":"+download_url+str(rangeHeader)].complete != 1:
					self.bcfslog.debug("waiting for aheadBuffer to fill: "+str(rangeHeader)+" "+str(sleepCnt))
					time.sleep(0.01)
					sleepCnt += 1
					if sleepCnt > 300:
						self.bcfslog.error("Error (download_file_part) aheadBuffer wait timeout: "+str(rangeHeader)+" "+str(sleepCnt))
						return
				self.aheadBuffer_mutex.acquire()
				self.aheadBuffer_lockedByThread = threading.current_thread().ident
				self.bcfslog.debug("acquire aheadBuffer_mutex")
				return_data = self.aheadBuffer[client_pid+":"+download_url+str(rangeHeader)].data
				self.aheadBuffer.pop(client_pid+":"+download_url+str(rangeHeader), None)
				self.aheadBuffer_lockedByThread = 0
				self.bcfslog.debug("release aheadBuffer_mutex")
				self.aheadBuffer_mutex.release()
				endLoop = 0
				return return_data
			else:
				self.aheadBuffer_lockedByThread = 0
				self.bcfslog.debug("release aheadBuffer_mutex")
				self.aheadBuffer_mutex.release()
				if ((offset + size) > total_size) or (self.bufferSizeCnt[client_pid+":"+download_url+":"+str(size)] < 3):
					# ADD SINGLE GET
					self.aheadBuffer_mutex.acquire()
					self.bcfslog.debug("acquire aheadBuffer_mutex")
					self.aheadBuffer_lockedByThread = threading.current_thread().ident
					if (client_pid+":"+download_url+str(rangeHeader) not in self.aheadBuffer) and (rangeHeader != None):
						self.createBuffer(download_url, rangeHeader, size, client_pid)
						self.bcfslog.debug("add range header: "+str(rangeHeader)+" Singleton size:"+str(size))
						job = DownloadChunk(download_url, [rangeHeader], self, size, size, start_byte, end_byte, client_pid, self.bcfslog)
						self.pool.put(job)
					self.aheadBuffer_lockedByThread = 0
					self.bcfslog.debug("release aheadBuffer_mutex")
					self.aheadBuffer_mutex.release()
					self.pool.wait()
					self.bcfslog.debug("single wait finished: "+str(rangeHeader)+" Singleton size:"+str(size))
				else:
					# ADD MULTIPLE RANGES
					self.download_pause_mutex.acquire()
					if self.download_pause == 0:
						self.download_pause = 1
						self.download_pause_mutex.release()	
						self.bcfslog.debug("Multiple add job: "+str(rangeHeader)+" size:"+str(size)+" count:"+str(self.bufferSizeCnt[client_pid+":"+download_url+":"+str(size)]))
						# Append already calculated
						if client_pid+":"+download_url+str(rangeHeader) not in self.aheadBuffer:
							new_offset = offset
							for work in range(self.NUM_WORKERS):
								if new_offset < total_size:
									start_byte = new_offset
									ranges = []
									max_chunks = int((total_size - new_offset) / size)
									if max_chunks > 0:
										if max_chunks > self.NUM_CHUNKS:
											max_chunks = self.NUM_CHUNKS
										end_byte = start_byte + ((max_chunks * size)-1)
										self.bcfslog.debug("Multiple add job: "+str(rangeHeader)+" max_chunks:"+str(max_chunks)+" start_byte:"+str(start_byte)+" end_byte:"+str(end_byte))
										self.aheadBuffer_mutex.acquire()
										self.bcfslog.debug("acquire aheadBuffer_mutex")
										self.aheadBuffer_lockedByThread = threading.current_thread().ident
										for chunk in range(max_chunks):
											new_rangeHeader = {'Range':'bytes='+str(new_offset)+'-'+str(new_offset+(size-1))}
											if (download_url+str(new_rangeHeader) not in self.aheadBuffer) and (new_rangeHeader != None):
												self.createBuffer(download_url, new_rangeHeader, size, client_pid)
											ranges.append (new_rangeHeader)
											new_offset = new_offset+size
										self.aheadBuffer_lockedByThread = 0
										self.bcfslog.debug("release aheadBuffer_mutex")
										self.aheadBuffer_mutex.release()
										self.bcfslog.debug("Multiple add job: start downloader process:"+str(work)+", ranges: "+str(ranges))
										tstart = datetime.datetime.now()
										job = DownloadChunk(download_url, ranges, self, size, size, start_byte, end_byte, client_pid, self.bcfslog)
										self.pool.put(job)
										tdelta = datetime.datetime.now() - tstart
										self.bcfslog.debug("Multiple add job: added (took "+str(int(tdelta.total_seconds()*1000))+"ms):"+str(work)+", ranges: "+str(ranges))
							endLoop += 1
							if endLoop >  4:
								self.bcfslog.error("Error: download_file_part max endLoop exceeded: "+str(endLoop))
						else:
							self.bcfslog.debug("Multiple add job: buffer already created, skipping "+str(rangeHeader))
						self.download_pause = 0
					else:
						self.download_pause_mutex.release()	
						time.sleep(0.01)
						self.bcfslog.debug("Multiple add job - download pause "+str(rangeHeader)+" size:"+str(size)+" count:"+str(self.bufferSizeCnt[client_pid+":"+download_url+":"+str(size)]))
		self.bcfslog.debug("download finished "+str(rangeHeader)+" size:"+str(size))

	def createBuffer (self, download_url, rangeHeader, chunk_size, client_pid):
		self.bcfslog.debug("starting "+str(rangeHeader)+" size:"+str(chunk_size))
		newBuf = namedtuple('newBuf', 'data, complete, chunk_size, last_accessed')
		newBuf.complete = 0
		newBuf.chunk_size = chunk_size
		newBuf.last_accessed = time.time()
		if client_pid+":"+download_url+str(rangeHeader) not in self.aheadBuffer:
			self.bcfslog.debug("create buffer:"+str(rangeHeader)+" size:"+str(chunk_size))
			self.aheadBuffer[client_pid+":"+download_url+str(rangeHeader)] = newBuf
		else:
			self.bcfslog.error("Error create buffer:"+str(rangeHeader)+" size:"+str(chunk_size)+" already exists")


	def download_file_url (self, file_id, path, file_name, file_size):
		file_url = self.api_path + "/files/"+file_id+"/"+ urllib.quote_plus(file_name) +"?access_token=" + self.access_token + "&path=/" + path
		self.bcfslog.debug("starting File URL: " + file_url)
		return file_url


	def upload_file (self, path, file_name, file_size):
		return
