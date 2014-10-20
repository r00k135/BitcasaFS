BitcasaFS
=========

A Bitcasa FUSE Filesystem implementation in Python.

Updated to work with the current API


Python Packages Required
========================
~~~
apt-get install python-fuse python-pip   # ubuntu
pip install urllib3
pip install workerpool
pip install requests
~~~


Usage
=====

Update config.json, using instructions from here to get an OAUTH based access_token: https://developer.bitcasa.com/docs#auth

Client - run client command to create a directory in the root folder
~~~
python client.py
~~~

Fuse Filesystem - mount a fuse filesystem
~~~
mkdir myfs
python bitcasafs.py myfs -o allow_other
find myfs /# this is necessary to cache the filesystem, hope to remove this
/# access as normal filesystem - may be slow due to HTTPS comms
/# umount when finished
fusermount -u myfs
~~~


Status Server
=============
Add an http server in a seperate thread so that you can monitor the driver with an REST API/HTTP requests, default port is 9090. Commands are below (using curl):
~~~
curl http://127.0.0.1:9090/bcfsapi20140823/bcfs_thread_status     # get status of all threads
curl http://127.0.0.1:9090/bcfsapi20140823/bcfs_buffer_sizes      # get size of readAhead buffer and block track buffer
~~~

There is also an HTML dashboard available in the docroot on:
~~~
http://127.0.0.1:9090/
~~~

Reference Docs
==============
I have used the following sources to write this:
~~~
https://launchpad.net/fuse-python-docs
http://www.stavros.io/posts/python-fuse-filesystem/
~~~
