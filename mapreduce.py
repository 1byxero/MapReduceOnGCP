#!/bin/python3

import os
import time
import signal
import argparse
import pickle
import xmlrpc.client

from subprocess import Popen
from comm_client import Communication_Client
from gcp import *

# KVPORT = 6055
# KVSERVER = "127.0.0.1"
# KVSERVER = "192.168.0.14"

MAPPEROPFOLDER = "mapper_op"
MAPPER_IP_KEY_NAME = "mapper_{}_ip_keys"
REDUCER_IP_FILE_NAME = "reducer_{}_ip"
MAPPER_OP_FILE_NAME = "keys_from_mapper_{}.txt"

KVSTORE_SPAWN_COMMAND = "./start_kv_store.py {kvstore_ip} {kvstore_port}"
MAPPER_SPAWN_COMMAND = "./mapper.py {ipkeys} {mapperid} {reducer_count} {map_function_file} {kvstore_ip} {kvstore_port}"
REDUCER_SPAWN_COMMAND = "./reduce.py {reducer_id} {reduce_function_file} {kvstore_ip} {kvstore_port}"
MASTER_OP_KEY = 'mapreduce_output'
PORT_CONSTANT = 8888

MAPPER_WORKER_TYPE = 'mapper'
REDUCER_WORKER_TYPE = 'reducer'
KVSTORE_WORKER_TYPE = 'kvstore'

class Master(object):

	def __init__(
			self, mapper_count, reducer_count, input_file, 
			map_function_file, reduce_function_file, 
			kvstore_ip, kvstore_port, mapper_service_ip,
			mapper_service_port, reducer_service_ip, reducer_service_port,
		):
		#get input, mapper function, mapper count, reduce fuction reducer cout 
		# self.kvstore_ip = kvstore_ip
		# self.kvstore_port = kvstore_port
		# self.mapper_service_ip = mapper_service_ip
		# self.mapper_service_port = mapper_service_port
		# self.reducer_service_ip = reducer_service_ip
		# self.reducer_service_port = reducer_service_port
		
		# self.start_kv_store()
		# verify kv store started:
		self.worker_lookup = self.start_workers(KVSTORE_WORKER_TYPE, 1)
		self.kvstore_ip = self.worker_lookup[KVSTORE_WORKER_TYPE]
		self.kvstore_port = PORT_CONSTANT
		self._check_kv_store_rpc_is_up()

		#serialize map function
		mapfn_serialized = self._serialize_functions(map_function_file)
		#serialize reduce function
		reducefn_serialized = self._serialize_functions(reduce_function_file)
		self._store_serilized_functions('key_for_mapfn', mapfn_serialized)
		self._store_serilized_functions('key_for_redfn', reducefn_serialized)

		self.unique_keys = set()
		self.mapper_count = mapper_count
		self.reducer_count = reducer_count
		self.map_function_file = map_function_file
		self.reduce_function_file = reduce_function_file
		
		try:
			self.worker_lookup = self.start_workers(MAPPER_WORKER_TYPE, self.mapper_count)
			mapper_process_objs = self._start_mappers(input_file)
			self._wait_on_mappers(mapper_process_objs)
			# TODO delete mapper worker types
			# Start reducers
			self.worker_lookup = self.start_workers(REDUCER_WORKER_TYPE, self.reducer_count)
			reducers_process_objs = self._start_reducers()
			self._wait_on_reducers(reducers_process_objs)
			# TODO delete reducer workers
			# self._remove_mapper_op_folder()		
			self._print_map_reduce_output()
		except:
			raise
		finally:
			# self._kill_kvstore_process()
			pass

	def start_kv_store(self):
		self.kvstore_proc = Popen(
			KVSTORE_SPAWN_COMMAND.format(
				kvstore_ip=self.kvstore_ip, kvstore_port=self.kvstore_port
			), 
			shell=True, preexec_fn=os.setsid
		)

	def _check_kv_store_is_up(self):
		# verify kv store started:
		kvstore_started = False
		while not kvstore_started:
			#Add limited retries
			try:
				sock = Communication_Client(self.kvstore_ip, self.kvstore_port)
				sock.handle_get("RandomKey")
				kvstore_started = True
			except ConnectionRefusedError:
				pass

	def _check_kv_store_rpc_is_up(self):
		# verify kv store started:
		kvstore_started = False
		print("Checking if  kv store has started")
		while not kvstore_started:
			#Add limited retries
			time.sleep(2)
			try:
				s = xmlrpc.client.ServerProxy(
					'http://{}:{}'.format(self.kvstore_ip, self.kvstore_port)
				)
				s.get_key("RandomKey")
				kvstore_started = True
				print("Verified kv store has started")
			except ConnectionRefusedError:
				pass


	def _start_mappers(self, input_file):
		#read split input and give it to mapper process
		read_ips = []
		input_per_mapper = []
		with open(input_file, "r") as f:
			for line in f.readlines():
				read_ips.append(line.strip())

		step = len(read_ips) // self.mapper_count
		step = 1 if step == 0 else step
		

		mapper_ip_key_list = []
		for ix, i in enumerate(range(0, len(read_ips), step)):
			mapper_ip_key = MAPPER_IP_KEY_NAME.format(ix)
			for item in read_ips[i:i+step]:
				sock = Communication_Client(self.kvstore_ip, self.kvstore_port)
				# TODO add retries if set fails
				sock.handle_set(mapper_ip_key, item)
			mapper_ip_key_list.append(mapper_ip_key)

		mapper_process_objs = []
		for ix, ipkeys in enumerate(mapper_ip_key_list):
			mapper_name = 'mapper-{}'.format(ix)
			mapper_ip = self.worker_lookup[mapper_name]
			s = self.get_xml_rpc_client(mapper_ip, PORT_CONSTANT)
			num = s.start_mapper(
				ipkeys, ix, self.reducer_count, 
				self.kvstore_ip, self.kvstore_port
			)
			mapper_process_objs.append((mapper_ip, num))
		return mapper_process_objs

	def _wait_on_mappers(self, mapper_process_objs):
		wait_for_mappers_to_finish = True
		print("wait_for_mappers_to_finish")
		while wait_for_mappers_to_finish:
			# saving some cpu cycles by using sleep
			time.sleep(5)
			wait_for_mappers_to_finish = False
			for obj in mapper_process_objs:
				mapper_ip, num = obj
				s = self.get_xml_rpc_client(mapper_ip, PORT_CONSTANT)
				pollval = s.check_if_mapper_done(num)
				if pollval == False:
					# process still executing
					wait_for_mappers_to_finish = True
		print("All Mappers done")

	def _start_reducers(self):
		reducers_process_objs = []
		for reducer_id in range(self.reducer_count):
			reducer_name = 'reducer-{}'.format(reducer_id)
			reducer_ip = self.worker_lookup[reducer_name]
			s = self.get_xml_rpc_client(reducer_ip, PORT_CONSTANT)
			num = s.start_reducer(reducer_id, self.kvstore_ip, self.kvstore_port)
			reducers_process_objs.append((reducer_ip, num))

		return reducers_process_objs

	def _wait_on_reducers(self, reducers_process_objs):
		wait_for_reducers_to_finish = True
		print("wait_for_reducers_to_finish")
		while wait_for_reducers_to_finish:
			# saving some cpu cycles using sleep
			time.sleep(5)
			wait_for_reducers_to_finish = False
			for obj in reducers_process_objs:
				reducer_ip, num = obj
				s = self.get_xml_rpc_client(reducer_ip, PORT_CONSTANT)
				pollval = s.check_if_reducer_done(num)
				if pollval == False:
					# process still executing
					wait_for_reducers_to_finish = True
		print("Reducers Done")

	def _remove_mapper_op_folder(self):
		if os.path.isdir(MAPPEROPFOLDER):
			for file in os.listdir(MAPPEROPFOLDER):
				flpth = os.path.join(MAPPEROPFOLDER, file)
				os.system(f"rm {flpth}")
			os.system(f"rmdir {MAPPEROPFOLDER}")

	def _kill_kvstore_process(self):
		if self.kvstore_proc.poll() == None:
			#kv store still running
			os.killpg(os.getpgid(self.kvstore_proc.pid), signal.SIGTERM)

		if self.kvstore_proc.poll() != None:
			print("KV Store stopped")

	def _serialize_functions(self, fnfile):
		fncontent = None
		with open(fnfile, 'r') as f:
			fncontent = f.read()
		return fncontent

	def _store_serilized_functions(self, key, fnobj):
		sock = Communication_Client(self.kvstore_ip, self.kvstore_port)
		# TODO add retries if set fails
		sock.handle_set(key, fnobj)

	def _print_map_reduce_output(self):
		sock = Communication_Client(self.kvstore_ip, self.kvstore_port)
		keys_to_read = sock.handle_get(MASTER_OP_KEY)
		print("----------------------Output from Map Reduce--------------------------")
		for key in keys_to_read:
			value = sock.handle_get(key)
			value = value[0] if value else None
			print(
				"key: {}, value: {}".format(
					key.replace('reducer_op_for_', ''), value
				)
			)

	def start_workers(self, instance_type, count):
		return start_instances(instance_type, count)

	def get_xml_rpc_client(self, ip, port):
		return xmlrpc.client.ServerProxy(
			'http://{}:{}'.format(
				ip, port
			)
		)
		client_started = False
		print("Checking if {}:{} is online".format(ip, port))
		while not client_started:
			#Add limited retries
			time.sleep(2)
			try:
				s = xmlrpc.client.ServerProxy(
					'http://{}:{}'.format(ip, port)
				)
				client_started = s.poll()
				print("{}:{} has started".format(ip, port))
				return s
			except ConnectionRefusedError:
				pass




parser = argparse.ArgumentParser()
parser.add_argument(
	'input_file', type=str, help='path to the input file for the map function'
)
parser.add_argument(
	'mapper_count', type = int,
	help='number of mappers to spawn'
)
parser.add_argument(
	'reducer_count', type = int,
	help='number of reducers to spawn '
)

parser.add_argument(
	'map_function_file', type = str,
	help='path to map function '
)

parser.add_argument(
	'reduce_function_file', type = str,
	help='path to reduce function '
)

parser.add_argument(
	'kvstore_ip', type = str,
	help='ip to spawn the key value store'
)

parser.add_argument(
	'kvstore_port', type = int,
	help='port to spawn the key value store'
)

parser.add_argument(
	'mapper_service_ip', type = str,
	help='ip of mapper rpc service'
)

parser.add_argument(
	'mapper_service_port', type = int,
	help='port of mapper rpc service'
)

parser.add_argument(
	'reducer_service_ip', type = str,
	help='ip of reducer rpc service'
)

parser.add_argument(
	'reducer_service_port', type = int,
	help='port of reducer rpc service'
)

args = parser.parse_args()

Master(
	args.mapper_count, args.reducer_count, args.input_file,
	args.map_function_file, args.reduce_function_file, 
	args.kvstore_ip, args.kvstore_port, args.mapper_service_ip, 
	args.mapper_service_port, args.reducer_service_ip, args.reducer_service_port, 
)
