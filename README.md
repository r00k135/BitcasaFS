BitcasaFS
=========

A Bitcasa FUSE Filesystem implementation in Python.

Updated to work with the current API

Usage
=====

Client - run client command to create a directory in the root folder
python client.py

Fuse Filesystem - mount a fuse filesystem
mkdir myfs
python bitcasafs.py myfs -o allow_other
# access as normal filesystem - may be slow due to HTTPS comms
# umount when finished
fusermount -u myfs
