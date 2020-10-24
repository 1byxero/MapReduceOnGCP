import socket
import time
import threading
import sys
import random
import json
import xmlrpc.client

class Communication_Client(object):
    HEADER = 64
    FORMAT = 'utf-8'
    DISCONNECT_MESSAGE = "!DISCONNECT"
    
    NOT_STORED = "NOT-STORED\r\n"
    STORED = "STORED\r\n"
    END = "END\r\n"
    split_key_val = ':::::'


    """docstring for Communication_Client"""
    def __init__(self, server, port):
        self.ADDR = (server, port)

    def communicat_header(self, sock, msg_len):
        send_length = str(msg_len).encode(self.FORMAT)
        send_length += b' ' * (self.HEADER - len(send_length))
        sock.send(send_length)


    def send(self, sock, msg):
        message = msg.encode(self.FORMAT)
        self.communicat_header(sock, len(message))
        sock.send(message)

    def receive(self, sock, size):
        msg_length = sock.recv(self.HEADER).decode(self.FORMAT)
        if not msg_length:
            return 
        msg_length = int(msg_length)       
        msg = sock.recv(msg_length).decode(self.FORMAT)
        return msg
        # return sock.recv(size).decode(self.FORMAT)

    def handle_set(self, key, value):
        #TODO retry of set key? -> check if else block

        # handling empty keys 
        if (not str(key)) or (not str(value)):
            return True 
        s = xmlrpc.client.ServerProxy(
                'http://{}:{}'.format(self.ADDR[0], self.ADDR[1])
            )
        return s.set_key(key, value)
        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock.connect(self.ADDR)

        # sz = sys.getsizeof(value)
        # sendstr = "set {} {}\r\n{}\r\n".format(key, sz, value)
        # self.send(sock, sendstr)
        # resp = self.receive(sock, sys.getsizeof(self.NOT_STORED))
        # if resp == self.STORED:
        #     # print( f"{i} Success storing {key}: {value}")
        #     return True
        # else:
        #     #to add retry logic here
        #     print( f"Unable to store {key}: {value}") 
        #     return False

    def handle_get(self, key):
        s = xmlrpc.client.ServerProxy(
            'http://{}:{}'.format(self.ADDR[0], self.ADDR[1])
        )
        try:
            valuestr = s.get_key(key)
            value = json.loads(valuestr)
        except json.decoder.JSONDecodeError:
            print('json.decoder.JSONDecodeError in kvstore', valuestr)
        return value if value else None
        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock.connect(self.ADDR)

        # self.send(sock, f"get {key}")
        # stuff = self.receive(sock, 4096)
        # splitstuff = stuff.split("\r\n")
        # valuestr = splitstuff[1].strip()    
        # # value = value.split(self.split_key_val)
        # try:
        #     value = json.loads(valuestr)
        # except json.decoder.JSONDecodeError:
        #     print('json.decoder.JSONDecodeError in kvstore', valuestr)
        
        # return value if value else None

        
        