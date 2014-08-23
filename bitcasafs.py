# BitcasaFS - A FUSE Filesystem Driver for BitcasaFS
# Original by: Michael Thomas, 2013 
# Updated by Chris Elleman (@chris_elleman), 2014

# Imports
import os, signal
import errno
import fuse
import stat
import time
import pprint
from bitcasa import Bitcasa
import logging
import logging.handlers

fuse.fuse_python_api = (0, 2)

class MyStat(fuse.Stat):
  def __init__(self):
    self.st_mode = stat.S_IFDIR | 0755
    self.st_ino = 0
    self.st_dev = 0
    self.st_nlink = 2
    self.st_uid = 0
    self.st_gid = 0
    self.st_size = 4096
    self.st_atime = 0
    self.st_mtime = 0
    self.st_ctime = 0

# Since Bitcasa API doesn't have real "permissions", lie.
class BitcasaStat(fuse.Stat):
	def __init__(self, item = ""):
		fuse.Stat.__init__(self)
		#print 'called BitcasaStat:',item

		# Check to see if item is file or folder
		if(item['Type'] == 'folders'):
			self.st_mode = stat.S_IFDIR | 0755
			self.st_nlink = 2
			self.st_size = 4096
		else:
			self.st_mode = stat.S_IFREG | 0666
			self.st_nlink = 1
			self.st_size = item['Size']
		# TODO: Find something to set these with.
		self.st_uid = os.geteuid()
		self.st_gid = os.getgid()

		# TODO: Bitcasa needs to save other times. Also, /1000 since they use microtime
		self.st_atime = int(item['mtime']) / 1000;
		self.st_mtime = int(item['mtime']) / 1000;
		self.st_ctime = int(item['mtime']) / 1000;

class BitcasaFS(fuse.Fuse):
	def __init__(self, *args, **kw):
		fuse.Fuse.__init__(self, *args, **kw)
		print "Fuse Init"
		
		# Set-Up Logger
		self.bcfslog = logging.getLogger(__name__)
 		self.bcfslog.setLevel(logging.DEBUG)
		handler = logging.handlers.SysLogHandler(address = '/dev/log', facility=logging.handlers.SysLogHandler.LOG_LOCAL6)
		formatter = logging.Formatter('Thead:%(thread)d(%(threadName)s) Function:%(module)s.%(funcName)s: %(message)s')
		handler.setFormatter(formatter)
 		self.bcfslog.addHandler(handler)

		# Python FUSE Options
		self.bitcasa = Bitcasa('config.json', self.bcfslog)
		if(self.bitcasa == None):
			sys.exit("Failed to authenticate Bitcasa Client.")
		print "Authenticated fine"
		# Bitcasa Encoded Path (for things like rename/create/delete)
		self.bpath = ""
		# Breadcrumbs to how we got where we are.
		self.breadcrumbs = {}
		# Files/Folders in Current Path
		self.dir = {}

	def getattr(self, path):
		print 'called getattr:', path
		if (path == '/'):
			t = MyStat();
			t.st_atime = int(time.time())
			t.st_mtime = t.st_atime
			t.st_ctime = t.st_atime
			return t
		# Else pass File/Folder Object
		# mkdir Check
		elif(path.split('/')[-1] in self.dir):
			return BitcasaStat(self.dir[path.split('/')[-1]])
		else:
			# DNE
			return -errno.ENOENT

	# Directory Methods
	def readdir(self, path, offset):
		# Turn English into Bitcasa Base64
		# Root Path - This clears our breadcrumbs @ preps
		if(path == "/"):
			# Get Files/Folders for Root
			bdir = self.bitcasa.list_folder(path)
			# Clear Breadcrumbs
			self.breadcrumbs = { }
			# Add Root Breadcrumb
			self.breadcrumbs['/'] = { 'Name':'', 'Path':'/', 'Type':'folders', 'mtime':'0'}
			# Reset Path
			self.bpath = ""
		else:
			# Load next round of Files/Folders from Bitcasa
			bdir = self.bitcasa.list_folder(self.dir[path.split('/')[-1]]['Path'])
			# Add our new Breadcrumb
			# TODO - add logic to check and see if we have this breadcrumb already.
			self.breadcrumbs[path.split('/')[-1]] = self.dir[path.split('/')[-1]]
			# Current Bitcasa Path
			self.bpath = self.dir[path.split('/')[-1]]['Path']
			print(self.bpath)
		for b in bdir:
			# Get Extra File Stuff
			if(b['category'] == 'folders'):
				item = { 'Name':b['name'].encode('utf-8'), 'Path':b['path'].encode('utf-8'), 'Type':b['category'], 'mtime':b['mtime'] }
			else:
				item = { 'Name':b['name'].encode('utf-8'), 'Path':b['path'].encode('utf-8'), 'Type':b['category'], 'mtime':b['mtime'], 'ID':b['id'].encode('utf-8'), 'Size':b['size'] }
			self.dir[b['name'].encode('utf-8')] = item
			# Now yield to FUSE
			yield fuse.Direntry(b['name'].encode('utf-8'))

	#def mkdir(self, path, mode):
	#	result = self.bitcasa.add_folder(self.bpath, path.split('/')[-1])
	#	if(result['error'] == None):
	#		new = result['result']['items'][0]
	#		item = { 'Name':new['name'].encode('utf-8'), 'Path':new['path'].encode('utf-8'), 'Type':new['category'], 'mtime':new['mtime'] }
	#		self.dir[new['name'].encode('utf-8')] = item
	#		return 0
	#	else:
	#		return -errno.ENOSYS

	# WIP - Doesn't report "file not found"
	#def rmdir(self, path):
	#	print("Removing " + path + ";CurrentPath: " + self.bpath)
	#	result = self.bitcasa.delete_folder(self.dir[path.split('/')[-1]]['Path'])
	#	if(result['error'] == None):
	#		return 0
	#	else:
	#		return -errno.ENOSYS

	# File Methods
	def open(self, path, flags):
		#pprint.pprint(self.dir)
		#print "Trying to open: ", path + "/" + self.dir[path.split('/')[-1]]['ID']
		print "open started: filename is ", self.dir[path.split('/')[-1]]['Name']+" Client Pid:"+str(self.GetContext()['uid'])
		download_url = self.bitcasa.download_file_url(self.dir[path.split('/')[-1]]['ID'], self.dir[path.split('/')[-1]]['Path'], self.dir[path.split('/')[-1]]['Name'], self.dir[path.split('/')[-1]]['Size'])
		self.dir[path.split('/')[-1]]['DownloadURL'] = download_url
		#temp_file = self.bitcasa.download_file(self.dir[path.split('/')[-1]]['ID'], self.dir[path.split('/')[-1]]['Path'], self.dir[path.split('/')[-1]]['Name'], self.dir[path.split('/')[-1]]['Size'])
		#temp_file = self.bitcasa.cache_dir + "/" + self.dir[path.split('/')[-1]]['Name']
		if download_url != None:
			return None
		else:
			return -errno.EACCESS

	# Read using streaming
	def read(self, path, size, offset, fh=None): 
		self.bcfslog.debug("read started: "+path+" offset:"+str(offset)+" size:"+str(size))
		return self.bitcasa.download_file_part(self.dir[path.split('/')[-1]]['DownloadURL'], offset, size, self.dir[path.split('/')[-1]]['Size'], str(self.GetContext()['uid']))

	# Release the file after reading is done
	def release(self, path, flags, fh=None):
		#ToDo flush aheadBuffer looking for keys with the client pid
		self.bcfslog.debug("release file: "+str(path)+", buffer still allocated:"+str(len(self.bitcasa.aheadBuffer))+" Client Pid:"+str(self.GetContext()['uid']))

	def flush(self, path, fh=None):
		self.bcfslog.debug("flush: "+str(path)+", buffer still allocated:"+str(len(self.bitcasa.aheadBuffer))+" Client Pid:"+str(self.GetContext()['uid']))
		return 0

	def fsdestroy(self):
		self.bcfslog.debug("destroy")
		self.bitcasa.pool.shutdown()


def handler(signum, frame):
	fh = open("/tmp/test", "wb")
	fh.write (str(time.time()))
	fh.close()
	print "Signal handler called with signal", signum

# return -errno.ENOENT
if __name__ == '__main__':
	fs = BitcasaFS()
	fs.parse(errex=1)
	#signal.signal(signal.SIGQUIT, signal.SIG_DFL)
	signal.signal(signal.SIGQUIT, handler)
	fs.main() # blocking call
