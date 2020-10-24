#!/bin/python3

import argparse
import threading

from reduce import Reducer
from xmlrpc.server import SimpleXMLRPCServer


parser = argparse.ArgumentParser()
parser.add_argument(
    'reducer_rpc_ip', type = str,
    help='IP of reducer rpc'
)

parser.add_argument(
    'reducer_rpc_port', type = int,
    help='port of reducer rpc'
)

args = parser.parse_args()

thread_lookup = {}

def start_reducer(
    reducer_number, kvstore_ip, kvstore_port
):
    obj = Reducer(
        reducer_number, (kvstore_ip, kvstore_port)
    )
    thread = threading.Thread(target=obj.start_work)
    thread.start()
    thread_lookup[reducer_number] = thread
    return reducer_number

def check_if_reducer_done(reducer_number):
    if reducer_number in thread_lookup:
        is_alive = thread_lookup[reducer_number].is_alive()
        if is_alive == False:
            return True
        elif is_alive == True:
            return False
        else:
            #maybe zombie?: TODO handle this
            return None
    return None
    

server = SimpleXMLRPCServer((args.reducer_rpc_ip, args.reducer_rpc_port))
server.register_introspection_functions()
server.register_function(start_reducer)
server.register_function(check_if_reducer_done)
# Run the server's main loop
server.serve_forever()