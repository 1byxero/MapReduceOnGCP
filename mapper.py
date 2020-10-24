#!/bin/python3

import os
import time
import json
import argparse

from subprocess import Popen
from hashlib import sha1

from comm_client import Communication_Client

SPLITCHAR = "#$%$#"
MAPPEROPFOLDER = "mapper_op"
OPFILE = os.path.join(MAPPEROPFOLDER, "mapper_output_{map_fn_count}.txt")


OPFORMASTERFILE = "keys_from_mapper_{map_fn_count}.txt"
OPFORREDUCER = "reducer_id_{id}"

# MAPFNSPWANCOMMAND = "./{map_function_file} {flname} {count} {server_ip} {server_port}"
MAPFNSPWANCOMMAND = "./mapfn{mapper_number}.py {flname} {count} {server_ip} {server_port}"

class Mapper(object):

    def __init__(
        self, input_keys, kvstore, 
        mapper_number, reducer_count
    ):

        self.mapper_number = mapper_number
        self.unique_keys = set()
        self.server_ip = kvstore[0]
        self.server_port = kvstore[1]
        self.reducer_count = reducer_count
        self.input_keys = input_keys
        # self.start_work()
        
    def start_work(self):
        client = Communication_Client(self.server_ip, self.server_port)
        # get mapper serialized function
        mapfn = client.handle_get('key_for_mapfn')
        self._create_local_map_fn(mapfn[0])

        
        # get name of keys to work on for this mapper 
        files_to_read = client.handle_get(self.input_keys)

        # create folder for map functions to write data to
        if not os.path.isdir(MAPPEROPFOLDER):
            os.system(f"mkdir {MAPPEROPFOLDER}")

        for ix, file_name in enumerate(files_to_read):
            p = Popen(
                MAPFNSPWANCOMMAND.format(
                    mapper_number=self.mapper_number,
                    flname=file_name, count=self.mapper_number,
                    server_ip=self.server_ip, server_port=self.server_port,
                ), 
                shell=True
            )
            p.wait()
            # print("Done with Mapper:", ix+1)
            time.sleep(5)

        self.read_output_of_mapper()
        # self.write_unique_keys_for_master()
        self.send_keys_to_reducer()

        # pass input to the mapper function
        # read this output file of mapper and store keys in the kvstore
        # pass unique keys back to master?
        self._delete_local_map_fn()
        self._remove_mapper_op_folder()

    def read_output_of_mapper(self):
        words = []
        with open(OPFILE.format(map_fn_count=self.mapper_number), "r") as f:
            words = json.load(f)
            for word, val in words:
                self.write_key_value(word, val)
                self.unique_keys.add(word)


    def convertToReducerNumber(self, key):
        val = int(sha1(key.encode()).hexdigest(), 16) % self.reducer_count
        return val

    def send_keys_to_reducer(self):
        for word in self.unique_keys:
            reducer_number = self.convertToReducerNumber(word)
            reducer_key = OPFORREDUCER.format(id=reducer_number)
            self.write_key_value(reducer_key, word)


    def write_unique_keys_for_master(self):
        with open(
            OPFORMASTERFILE.format(map_fn_count=self.mapper_number), "w"
        ) as f:
            for word in self.unique_keys:
                f.writelines([word+"\n"])


    def write_key_value(self, k, v):
        client = Communication_Client(self.server_ip, self.server_port)
        client.handle_set(k, v)

    def _create_local_map_fn(self, contents):
        with open('mapfn{}.py'.format(self.mapper_number), 'w') as f:
            f.write(contents)
        os.system('chmod +x mapfn{}.py'.format(self.reducer_number))


    def _delete_local_map_fn(self):
        os.system('rm mapfn{}.py'.format(self.mapper_number))

    def _remove_mapper_op_folder(self):
        if os.path.isdir(MAPPEROPFOLDER):
            for file in os.listdir(MAPPEROPFOLDER):
                flpth = os.path.join(MAPPEROPFOLDER, file)
                os.system(f"rm {flpth}")
            os.system(f"rmdir {MAPPEROPFOLDER}")