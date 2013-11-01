#!/bin/bash

time while [ "$waiting" != "0" ]; do
	sleep 3
	elastic-mapreduce --describe $1 |grep "State.*WAITING" > /dev/null
	waiting=$?
done
