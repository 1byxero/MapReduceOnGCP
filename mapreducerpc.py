from xmlrpc.server import SimpleXMLRPCServer

import logging
import os

from mapreduce import Master

logging.basicConfig(level=logging.DEBUG)



def start_mapreduce():
	logging.debug('starting server')
	flname = 'file_to_read.txt'
	mcount = 2
	rcount = 10
	mapfn = 'word_count_map_function.py'
	reducefn = 'word_count_reduce_function.py'
	ip = '127.0.0.1'
	port = 8055
	Master(
		mcount, rcount, flname, mapfn, reducefn, ip, port
	)
	return True

server = SimpleXMLRPCServer(('localhost', 9000), logRequests=True)
server.register_introspection_functions()
server.register_function(start_mapreduce)
server.serve_forever()
