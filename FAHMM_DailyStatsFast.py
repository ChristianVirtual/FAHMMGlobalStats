#! /usr/bin/env python

#
# (c) Christian Lohmann
#
# Author: Christian Lohmann 2015
#
#
# License: take it, use it, change it ...
#
# when run as console the stdin can be used to terminate the script
# when run as daemon we should add a signal handler; right now I'm to lazy for that
#
# http://fah-web.stanford.edu/daily_user_summary.txt.bz2
# 
# SQLAlchemy
# APNS
# dateutil


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

#
#
#
def printcopyrightandusage():
	""" (c) Christian Lohmann, 2015, www.fahmm.net"""	

   	print "(c) Christian Lohmann 2015" 
   	print "Daily Statistics Collector and Notification Handler" 
   


if __name__ == '__main__':

	printcopyrightandusage()
	
	
	parser = argparse.ArgumentParser(description='Process some daily summary statistics.')
	#parser.add_argument('integers', metavar='N', type=int, nargs='+',
	#	help='an integer for the accumulator')
	parser.add_argument('-file', dest='filename', 
		help='filename with daily summary (default: daily_user_summary.txt')

	args = parser.parse_args()
	

	# connect to database
	conn = sqlite3.connect('fahmmds.db')
	c = conn.cursor()

	try:
		# create table
		#c.execute('''CREATE TABLE fah_stats_monthly (user TEXT, team INT, cntWU INT, sumCredit REAL, ts DATETIME)''') 
		#c.execute('''CREATE TABLE fah_stats_weekly (user TEXT, team INT, cntWU INT, sumCredit REAL, ts DATETIME)''') 
		c.execute('''CREATE TABLE fah_stats_daily (user TEXT, team INT, cntWU INT, sumCredit REAL, ts DATETIME)''') 
		c.execute('''CREATE TABLE fah_import_ds (user TEXT, team INT, cntWU INT, sumCredit REAL, ts DATETIME)''') 
		#c.execute('''CREATE TABLE fah_stats_daily (user TEXT, team INT, cntWU INT, sumCredit REAL, ts DATETIME, PRIMARY KEY (user, team, cntWU))''') 
		#c.execute('''CREATE TABLE fah_import_ds (user TEXT, team INT, cntWU INT, sumCredit REAL, ts DATETIME, PRIMARY KEY (user, team, cntWU))''') 

		c.execute('''CREATE INDEX IF NOT EXISTS idx_import_ut on fah_import_ds (user, team)''') 
		c.execute('''CREATE INDEX IF NOT EXISTS idx_daily_ut on fah_stats_daily (user, team)''') 
		
		c.execute('''CREATE TABLE fah_user_token (user TEXT, team INT, token TEXT)''') 
		c.execute('''CREATE INDEX IF NOT EXISTS idx_user_token on fah_user_token (user)''') 
		
	except sqlite3.OperationalError as e:
		print e

	c.execute('DELETE FROM fah_import_ds ') 
	conn.commit()
	
	# timestamp and start the loop
	tsFile = datetime.datetime.utcnow()
	clockStart = time.clock()
	i = 0

	importLines = []
	
	# open file and read line by line
	with open(args.filename) as f:
		for line in f:
			line1 = line.split('\n')
			if len(line1) > 0:
				if i == 0:
					ts = email.utils.parsedate(line1[0])
					tsFile = datetime.datetime.fromtimestamp(time.mktime(ts))
					# print line1[0], ts, tsFile
				elif i == 1:
					print 'start reading'
				else:
					try:
						ul = unicode(line1[0], errors='replace')
						col = ul.split('\t')
						importLines.append( (col[0], col[3], col[2], col[1], tsFile) )

					except UnicodeDecodeError as e:
						print e, line1[0]
			i = i + 1
			if (i % 1000) == 0:
				clockNow = time.clock()
				sec = (clockNow - clockStart) 
				lps = i / sec
				stdout.write("\r%d %s, line per sec %d" % (i, str(sec), lps))
				stdout.flush()
				
	c.executemany('INSERT INTO fah_import_ds VALUES (?, ?, ?, ?, ?)', importLines)
	conn.commit()
	
	#
	clockEnd = time.clock()
	print "\nfinish with import ", clockEnd - clockStart
	print 'start processing'
	importLines = []

	c.execute('select i.user,i.team,i.cntWU from fah_import_ds as i INNER JOIN fah_stats_daily as d ON i.user = d.user and i.team = d.team and i.cntWU = d.cntWU')
	rows = c.fetchall()
	print len(rows), " inactive entries"
	for row in rows:
		importLines.append((row[0], row[1], row[2]))
	c.executemany('DELETE from fah_import_ds where user=? and team=? and cntWU=?', importLines)
	conn.commit()
	
	# copy remaining entries over into daily table
	c.execute('insert into fah_stats_daily (user, team, cntWU, sumCredit, ts) select user, team, cntWU, sumCredit, ts from fah_import_ds')
	conn.commit()

	c.execute('vacuum')
	conn.commit()
	
	clockEnd = time.clock()
	print "\nfinish in ", clockEnd - clockStart
	conn.close()
	


