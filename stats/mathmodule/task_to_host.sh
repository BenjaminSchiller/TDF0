#!/bin/zsh
source `dirname $0`/uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat $@ > $tmpfile



grep -i task $tmpfile | cut -d, -f4,2 --output-delimiter=" " | uniq 

rm $tmpfile
