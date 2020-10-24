#!/bin/python3

import os
import json
import argparse

from comm_client import Communication_Client

# KVPORT = 5052
# KVSERVER = "127.0.0.1"
MASTER_OP_KEY = 'mapreduce_output'

class InvertedIndexReducFn(object):

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
		file_names = set()
		value = client.handle_get(self.key_to_use)
	
		for val in value:
			try:
				if val:
					file_names.add(val)
			except ValueError:
				# Need to understand why is value error coming
				print(type(val))
				print(val)
				print(value)

		self.send_op_to_kvstore(file_names)

	def send_op_to_kvstore(self, op):
		if not isinstance(op, str):
			op = str(op)
		client = Communication_Client(self.server_ip, self.server_port)
		key = 'reducer_op_for_{}'.format(self.key_to_use)
		client.handle_set(key, op)
		self.send_op_key_to_master(key)
	 

	def send_op_key_to_master(self, key):
		client = Communication_Client(self.server_ip, self.server_port)
		client.handle_set(MASTER_OP_KEY, key)
		


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

InvertedIndexReducFn(
	args.uid_of_key,
    (args.kvstore_ip, args.kvstore_port)
)