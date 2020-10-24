#!/bin/bash

sudo apt update
sudo apt install python3-pip
cd /home/anuj/
rm -rf MapReduceOnGCP
git clone https://github.com/1byxero/MapReduceOnGCP.git
cd MapReduceOnGCP
./start_kv_store.py 0.0.0.0 8888
