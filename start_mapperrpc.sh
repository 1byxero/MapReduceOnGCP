#!/bin/bash


sudo apt update
sudo apt install python3-pip
yes | pip3 install google-api-python-client
cd /home/anuj/
rm -rf MapReduceOnGCP
git clone https://github.com/1byxero/MapReduceOnGCP.git
cd MapReduceOnGCP
./mapperrpc.py 0.0.0.0 8888