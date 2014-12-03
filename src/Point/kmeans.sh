#!/bin/bash
if [ $# -eq 2 ]; then
	echo "Generate Points for $1 Clusters with $2 Points"
	python ./generaterawdata.py -c $1 -p $2 -o cluster.csv
else
	echo "Usage: ./kmeans.sh <NumOfCluser> <NumOfPoints>"
fi