#!/bin/bash

wget -O ~/workspace/data/off_raw/images/$1.jpg $2

# hdfs dfs -put -f  ~/workspace/data/off_raw/images/$1.jpg /user/ubuntu/off_raw/images