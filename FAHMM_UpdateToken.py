#! /usr/bin/env python

#
# (c) Christian Lohmann
#
# Author: Christian Lohmann 2015
#
#
# License: under MIT license 
#
# This module is collecting the JSON files containing user names and device token uses
# for Apple Push Notification and put them into an internal database tabel
#
#
# Required modules
# APNS	https://github.com/djacobs/PyAPNs
# dateutil	https://pypi.python.org/pypi/python-dateutil/
# SQLite3 database
#

import os
import sys
import time
import datetime
import dateutil.parser
import email.utils
import argparse

import socket
import select
import string
import sqlite3
#import requests
from thread import *

from sys import stdout


import json



activeClient = []


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
#
#
#
def printcopyrightandusage():
	""" (c) Christian Lohmann, 2015, www.fahmm.net"""	

   	print "(c) Christian Lohmann 2015" 
   	print "User & DeviceToken Management" 
   


if __name__ == '__main__':

	printcopyrightandusage()
	
	# connect to database
	conn = sqlite3.connect('fahmmds.db')
	
	c = conn.cursor()

	try:
		c.execute('''CREATE TABLE fah_user_token (user TEXT, team INT, token TEXT)''') 
		c.execute('''CREATE INDEX IF NOT EXISTS idx_user_token on fah_user_token (user)''') 
		
	except sqlite3.OperationalError as e:
		print e

	
	# timestamp and start the loop
	tsFile = datetime.datetime.utcnow()
	clockStart = time.clock()
	i = 0


	flist = []
	tokenList = []
	for (dirpath, dirnames, filenames) in os.walk('./token/'):
		for fn in filenames:
			json_data=open(dirpath+fn)
			data = json.load(json_data)
			json_data.close()
			print data
			tokenList.append((data['user'], -1, data['token']))

	c.executemany('INSERT or REPLACE INTO fah_user_token VALUES (?, ?, ?)', tokenList)
	conn.commit()

	
	
	#
	clockEnd = time.clock()
	print "\nfinish with import ", clockEnd - clockStart
	print 'start processing'

	# select fah_import_ds.user,fah_import_ds.team,fah_import_ds.sumCredit,fah_user_token.token from fah_user_token inner join fah_import_ds where fah_import_ds.user = fah_user_token.user;
	
	conn.row_factory = dict_factory
	c.execute("select fah_import_ds.user,fah_import_ds.team,fah_import_ds.sumCredit,fah_user_token.token from fah_user_token inner join fah_import_ds where fah_import_ds.user = fah_user_token.user;")
	results = c.fetchall()
 
	# select user,team,max(cntWU) from fah_stats_daily where user='ChristianFAH' group by user,team;
 
	print results
	
	print 'cleanup'
	c.execute('vacuum')
	conn.commit()
	
	clockEnd = time.clock()
	print "\nfinish in ", clockEnd - clockStart
	conn.close()
	


