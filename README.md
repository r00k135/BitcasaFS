BitcasaFS
=========

A Bitcasa FUSE Filesystem implementation in Python.

Updated to work with the current API


Python Packages Required
========================
apt-get install python-fuse   # ubuntu
pip install urllib3


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
/# access as normal filesystem - may be slow due to HTTPS comms
/# umount when finished
fusermount -u myfs
~~~
