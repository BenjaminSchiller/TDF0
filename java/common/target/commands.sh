#!/bin/bash

pwd=$(dirname $0)

echo $pwd

java -cp ${pwd}/commands.jar de.tuda.p2p.tdf.cmd.$(basename $0) $@
