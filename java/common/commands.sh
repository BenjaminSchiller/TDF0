#!/bin/bash

pwd=$(dirname $0)

java -cp ${pwd}/commands.jar de.tuda.p2p.tdf.cmd.$(basename $0) $@
