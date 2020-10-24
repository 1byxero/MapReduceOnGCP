import socket 
import threading
import sys
import os
import time
import json
from collections import defaultdict

class key_val_store(object):

    HEADER = 64
    FORMAT = 'utf-8'
    DISCONNECT_MESSAGE = "!DISCONNECT"
    
    store = defaultdict(list)


    def __init__(self, ip, port, rpc=False):
        self.server_ip = ip
        self.server_port = port
        self.threadlock = threading.Lock()

        print("[STARTING] server is starting...")
        if not rpc:
            self.start()
        else:
            self.start_rpc()

    def receive_msg(self, conn):
        msg_length = conn.recv(self.HEADER).decode(self.FORMAT)
        if not msg_length:
            return 
        msg_length = int(msg_length)       
        msg = conn.recv(msg_length).decode(self.FORMAT)
        return msg

    def send_msg(self, conn, msg):
        message = msg.encode(self.FORMAT)
        msg_length = len(message)
        send_length = str(msg_length).encode(self.FORMAT)
        send_length += b' ' * (self.HEADER - len(send_length))
        conn.send(send_length)
        conn.send(message)
        

    def write_key_value_pair_to_file(self, key, value):
        
        try:
            #acquire lock
            #write key val pair
            with self.threadlock:
                written = False
                self.store[key].append(value)
                written = True
            #return true false for succ or failure
        except Exception as e:
            #need to check what exceptions may arise
            raise e
        finally:
            return True if written else False 


    def read_key_value_pair_from_file(self, key):
        #acquire lock
        #read key val pair
        #release lock
        #return value if succ else return empty string for failure

        value = ""
        try:
            # with threadlock:
            with self.threadlock:
                stuff = self.store[key]
                value = json.dumps(stuff)
        except json.decoder.JSONDecodeError:
            print('json.decoder.JSONDecodeError in kvstore', self.store[key])
        except Exception as e:
            #need to check what exceptions may arise
            raise e
        finally:
            return value



    def set_key(self, key, value):
        try:
            if isinstance(value, str):
                value = value.strip()
            if self.write_key_value_pair_to_file(key, value):
                return True
            else:
                return False
        except:
            raise

    def get_key(self, key):
        try:
            return self.read_key_value_pair_from_file(key)
        except:
            #need to see what exeptions are going to come
            raise

    def handle_set(self, conn, key, value):
        if self.set_key(key, value):
            # conn.send(b"STORED\r\n")
            self.send_msg(conn, "STORED\r\n")
        else:
            # conn.send(b"NOT-STORED\r\n")
            self.send_msg(conn, "NOT-STORED\r\n")
        connected = False

    def handle_get(self, conn, key):
        value = self.get_key(key)
        sz = sys.getsizeof(value) if value != "" else 0
        retstr = "VALUE {} {}\r\n{}\r\nEND\r\n".format(
            key, sz, value
        )
        self.send_msg(conn, retstr)
        # conn.send(retstr.encode(self.FORMAT))


    def handle_client(self, conn, addr):
        # print(f"[NEW CONNECTION] {addr} connected.")
        try:
            connected = True
            while connected: 
                # msg = conn.recv(4096).decode(self.FORMAT)
                msg = self.receive_msg(conn)
                split_msg = msg.split("\r\n")
                try:
                    line_one_split = split_msg[0].split()
                    command = line_one_split[0]
                    key = line_one_split[1]
                except IndexError:
                    raise
                if len(split_msg) > 2 and command == 'set':
                    value = split_msg[1]
                    self.handle_set(conn, key, value)
                    connected = False
                elif command == 'get':
                    self.handle_get(conn, key)
                    connected = False
                else:
                    # invalid input command 
                    connected = False
                    
            conn.close()
        except:
            #need to check what all exceptions might come
            conn.close()
            raise

    def start_rpc(self):
        print(f"Server started")

        
        # while True:
        #     conn, addr = self.server.accept()
        #     thread = threading.Thread(target=self.handle_client, args=(conn, addr))
        #     thread.start()
        #     # print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

    def start(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ADDR = (self.server_ip, self.server_port)        
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(self.ADDR)
        
        self.server.listen()
        print(f"[LISTENING] Server is listening on {self.server_ip}:{self.server_port}")
        
        while True:
            conn, addr = self.server.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            thread.start()
            # print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")