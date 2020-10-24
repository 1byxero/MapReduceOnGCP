#!/bin/python3

import argparse
import threading

from mapper import Mapper
from xmlrpc.server import SimpleXMLRPCServer


parser = argparse.ArgumentParser()
parser.add_argument(
    'mapper_rpc_ip', type = str,
    help='IP of mapper rpc'
)

parser.add_argument(
    'mapper_rpc_port', type = int,
    help='port of mapper rpc'
)

args = parser.parse_args()

thread_lookup = {}

def start_mapper(
    input_keys, mapper_number, reducer_count,
    kvstore_ip, kvstore_port
):
    obj = Mapper(
        input_keys, (kvstore_ip, kvstore_port),
        mapper_number, reducer_count
    )
    thread = threading.Thread(target=obj.start_work)
    thread.start()
    thread_lookup[mapper_number] = thread
    return mapper_number

def check_if_mapper_done(mapper_number):
    if mapper_number in thread_lookup:
        is_alive = thread_lookup[mapper_number].is_alive()
        if is_alive == False:
            return True
        elif is_alive == True:
            return False
        else:
            #maybe zombie?: TODO handle this
            return None
    return None
    

server = SimpleXMLRPCServer((args.mapper_rpc_ip, args.mapper_rpc_port))
server.register_introspection_functions()
server.register_function(start_mapper)
server.register_function(check_if_mapper_done)
# Run the server's main loop
server.serve_forever()