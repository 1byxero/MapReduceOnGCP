#!/bin/python3

import os
import json
import argparse

from comm_client import Communication_Client

# KVPORT = 5052
# KVSERVER = "127.0.0.1"

class SkeletonReducer(object):

	def __init__(self, uid_of_key, kvstore):
		self.server_ip = kvstore[0]
		self.server_port = kvstore[1]
		self.key_to_use  = self._fetch_key_to_reduce(uid_of_key)
		self._do_the_work()

	def _fetch_key_to_reduce(self, uid_of_key):
		client = Communication_Client(self.server_ip, self.server_port)
		key_to_use = client.handle_get(uid_of_key)
		return key_to_use[0]

	def _do_the_work(self):
		client = Communication_Client(self.server_ip, self.server_port)
		word_count = 0	
		value = client.handle_get(self.key_to_use)
	
		for val in value:
			try:
				if val:
					word_count+=int(val)
			except ValueError:
				# Need to understand why is value error coming
				print(type(val))
				print(val)
				print(value)

		print(self.key_to_use, word_count)

parser = argparse.ArgumentParser()
parser.add_argument(
	'uid_of_key', type=str, help='Key to reduce'
)

parser.add_argument(
    'kvstore_ip', type = str,
    help='IP of kv store'
)

parser.add_argument(
    'kvstore_port', type = int,
    help='port of kv store'
)

args = parser.parse_args()

SkeletonReducer(
	args.uid_of_key,
    (args.kvstore_ip, args.kvstore_port)
)