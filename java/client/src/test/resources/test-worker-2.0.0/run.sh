#!/bin/sh

sleep 600 &

sleep 600 &

trap "kill 0" SIGINT SIGTERM EXIT

wait