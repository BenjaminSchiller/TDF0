#!/bin/zsh
source `dirname $0`/uniq.sh
source `dirname $0`/fails_over_time.sh.inc

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
#while [ "$2" ]; do
#shift
#done
cat "$@" > $tmpfile

fails_over_time $tmpfile 60000

rm $tmpfile 2>/dev/null
