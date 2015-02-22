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
# for Apple Push Notification and put them into an internal database tabel.
# It open a port and listen to JSON files data coming in containing the 
#	user name 
#	unique device token from iPhone/iPad
#
#
# Required modules
# APNS	https://github.com/djacobs/PyAPNs
# dateutil	https://pypi.python.org/pypi/python-dateutil/
#
#


import SimpleHTTPServer
import SocketServer
import logging
import cgi
import json

import sys


class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):
        logging.warning("* * * * * GET started * * * * *")
        logging.warning(self.headers)
        SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
        logging.warning("* * * * * POST started * * * * *")
        logging.warning(self.headers)
        
        
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })
        logging.warning("* * * * * POST values * * * * *")
        
        ut = json.loads(form.getvalue("ut"))
        
        logging.warning(ut)
        fn = './token/' + ut['token']+'.json'
        with open(fn, 'w') as the_file:
           the_file.write(json.dumps(ut))


PORT = 36336

Handler = ServerHandler

httpd = SocketServer.TCPServer(("", PORT), Handler)


print "(c) Christian Lohmann, simple token registration server version 0.5"
print "Listen on port ", PORT

httpd.serve_forever()