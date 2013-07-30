#!/bin/sh

# Kill the spawned Python process if the run script is aborted
trap "kill 0" SIGINT SIGTERM EXIT

# Start the worker and hand over the command line parameters
python worker.py $1 $2
