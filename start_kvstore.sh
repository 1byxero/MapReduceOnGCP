#!/bin/bash

cd /home/anuj/
rm -rf MapReduceOnGCP
git clone https://github.iu.edu/abgodase/MapReduceOnGCP.git
cd MapReduceOnGCP
./start_kv_store.py 0.0.0.0 8888
