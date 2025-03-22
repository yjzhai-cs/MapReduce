#! /bin/bash

if [ $1 == "update_dslogs" ]
then
    rm -rf /usr/bin/dslogs
    cp dslogs /usr/bin/
    chmod 777 /usr/bin/dslogs
if [ $1 == "update_dstest" ]
then
    rm -rf /usr/bin/dstest
    cp dstest /usr/bin/
    chmod 777 /usr/bin/dstest
elif [ $1 == "rm_log" ]
then
    rm -rf output.log
elif [ $1 == "dstest" ]
then
    python3 dstest -n 2000 -p 100 -v InitialElection ReElection ManyElections
fi