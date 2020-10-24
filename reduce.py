#!/bin/python3
import os
import time
import hashlib
import argparse

from subprocess import Popen

from comm_client import Communication_Client

# KVPORT = 5052
# KVSERVER = "127.0.0.1"
OPFORREDUCER = "reducer_id_{id}"

REDUCE_FN_SPAWN_COMMAND = "./redfn{reducer_number}.py {uid} {kvstore_ip} {kvstore_port}"

class Reducer(object):

    def __init__(self, reducer_number, kvstore):
        self.reducer_number = reducer_number
        self.server_ip = kvstore[0]
        self.server_port = kvstore[1]
        self.reducer_get_key = OPFORREDUCER.format(id=reducer_number)

    def start_work(self):
        client = Communication_Client(self.server_ip, self.server_port)
        # get mapper serialized function
        reducefn = client.handle_get('key_for_redfn')
        self._create_local_reduce_fn(reducefn[0])

        # self.keys_to_do = None
        # while self.keys_to_do == None:
        #     self.get_keys_to_work_on()
        #     time.sleep(5)
        #     print("Retrying")
        self.get_keys_to_work_on()
        
        # print(self.keys_to_do, "self.keys_to_do") 
        
        for ix, key in enumerate(self.keys_to_do):
            uid = self.store_hash_of_key(key)
            
            p = Popen(
                REDUCE_FN_SPAWN_COMMAND.format(
                    reducer_number=self.reducer_number,
                    uid=uid, kvstore_ip=self.server_ip, 
                    kvstore_port=self.server_port
                ), 
                shell=True,
            )
            p.wait()
            # print("Done with Reducer:", ix+1)

        self._delete_local_reduce_fn()

    def get_keys_to_work_on(self):
        client = Communication_Client(self.server_ip, self.server_port)
        self.keys_to_do = client.handle_get(self.reducer_get_key)

    def get_hash(self, key):
        h = hashlib.sha1()
        h.update(key.encode('utf-8'))
        return h.hexdigest()

    def store_hash_of_key(self, key):
        uid = self.get_hash(key)
        client = Communication_Client(self.server_ip, self.server_port)
        client.handle_set(uid, key)
        #TODO remove wait and add retries 
        # time.sleep(1)
        return uid

    def _create_local_reduce_fn(self, contents):
        with open('redfn{}.py'.format(self.reducer_number), 'w') as f:
            f.write(contents)

    def _delete_local_reduce_fn(self):
        os.system('rm redfn{}.py'.format(self.reducer_number))