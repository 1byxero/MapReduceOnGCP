#!/bin/python3

import argparse

from key_value_store import key_val_store
from xmlrpc.server import SimpleXMLRPCServer

parser = argparse.ArgumentParser()
parser.add_argument(
    'kvstore_ip', type = str,
    help='IP of kv store'
)

parser.add_argument(
    'kvstore_port', type = int,
    help='port of kv store'
)

args = parser.parse_args()
# -----------------------------------RPC-----------------------------------#
rpc = True
# -----------------------------------RPC-----------------------------------#
obj = key_val_store(args.kvstore_ip, args.kvstore_port, rpc=rpc)

server = SimpleXMLRPCServer((args.kvstore_ip, args.kvstore_port))
server.register_introspection_functions()
server.register_instance(obj)
# Run the server's main loop
server.serve_forever()


