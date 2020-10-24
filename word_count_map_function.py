#!/bin/python3

import os
import json
import time
import argparse

MAPPEROPFOLDER = "mapper_op"
OPFILE = os.path.join(MAPPEROPFOLDER, "mapper_output_{map_fn_count}.txt")
SPLITCHAR = "#$%$#"


class WordCount(object):

	def __init__(self, ipfile, map_fn_count, kvstore):
		self.ipfile = ipfile
		self.map_fn_count = map_fn_count
		self._do_the_work()
		self.kvserver = kvstore[0]
		self.kvport = kvstore[1]

	def get_basic_word(self, word):
		return ''.join(e for e in word if e.isalpha())

	def _do_the_work(self):
		content = []
		words = []
		with open(self.ipfile, "r") as f:
			content = f.readlines()

		opflname = OPFILE.format(map_fn_count=self.map_fn_count)
		

		if os.path.isfile(opflname):	
			with open(opflname, 'r') as f:
				words = json.load(f)

		for line in content:
			# words.extend(line.strip().split())
			for word in line.strip().split():
				words.append((word, 1))
		
		with open(opflname, 'w') as f:
			json.dump(words, f)
		#making sure file is written before read on the other end 
		# time.sleep(1)



parser = argparse.ArgumentParser()
parser.add_argument(
	'ipfile', type=str, help='path to the input file for the map function'
)
parser.add_argument(
	'mapper_number', type = int,
	help='number of this mapper '
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

WordCount(
	args.ipfile,
	args.mapper_number,
    (args.kvstore_ip, args.kvstore_port)
)