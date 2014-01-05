#!/bin/bash

time while [ "$completed" != "0" ]; do
	sleep 3
	elastic-mapreduce --describe $1 |grep "State.*COMPLETED" > /dev/null
	completed=$?
done
