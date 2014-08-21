BitcasaFS
=========

A Bitcasa FUSE Filesystem implementation in Python.

Updated to work with the current API


Python Packages Required
========================
apt-get install python-fuse   # ubuntu
pip install urllib3
pip install workerpool


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


Reference Docs
==============
I have used the following sources to write this:
~~~
https://launchpad.net/fuse-python-docs
http://www.stavros.io/posts/python-fuse-filesystem/
~~~