## File Structure
- The implementation is completely modularized and is distributed across various files
- mapreduce.py contains the implementation of master
- mapper.py contains the implementation of mapper
- reducer.py contains the implemetation of reducer
- key_value_store.py contains the datastore implementation
- comm_client.py contais the implementation for communication client
- skeleton_map_funcntion.py contains skeleton code for map function python file
- skeleton_reduce_funcntion.py contains skeleton code for reduce function python file

## Design and flow
1. Master takes in input file, number of mappers and reducers, the python files for map and reduce function
2. Master, starts the key value store
3. Master reads in the input and distributes it for mappers, this is stored in datastore in mapper_1: list(inputs for mapper_1) format
4. Master spawns all the mappers and passes map function file,  key to read from datastore for input, number of reducers, port and ip of data store as command line argument and master polls and waits for all the mappers to finish
5. Mapper function reads the input assigned, processes it stores in a local file. This file is then read by mapper and all the key value pairs are sent to the datastore along with data assigned for the reducer is sent to the datastore.
6. Once all the mappers are done processing, master get control again and spwans the reducers with appropriate reducer function. This reducer function gets the which key to read for its assigned input as command line argument along with datastore port and ip. Master waits for all reducers to finish working
7. Reducer function then gets all the keys it needs to process and prints the output to the console.
8. Once all the reducers are completed, the master shuts down the datastore

# Usage
**This implementation usage uses python3**
To start the map reduce use the following command
```
.//mapreduce.py <inputfile> <number_of_mappers> <number_of_reducers> <path to map function file> <path to reduce fuction file> <datastore ip> <datastore port>
```

# Test cases
1. This is example for inverted index

    ```./mapreduce.py file_to_read.txt 2 10 inverted_index_map_function.py inverted_index_reduce_function.py datastore_ip datastore_port```
should create output equivalent to text in file test_case2_expected_op.txt


2. This is word cout example

    ```./mapreduce.py file_to_read.txt 2 10 word_count_map_function.py word_count_reduce_function.py datastore_ip datastore_port```
should create output equivalent to text in file test_case1_expected_op.txt 


# How to extend for new map and reduce functions
To use this implementation for custom map reduce functions, just edit `_do_the_word` method in skeleton_map_function.py and skeleton_reduce_function and everything would work out accordingly.

# Assumptions
1. This implementation assuments that the master, mapper and reducers have enough resources to complete the assigned amount of task. 
2. Number of mapper and reducers is appropriate to achieve max parallelisation
3. data store runs on master node and it has enough resources to store all the data in memory
