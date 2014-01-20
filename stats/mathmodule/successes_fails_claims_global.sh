#!/bin/zsh 
source `dirname $0`/uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat $1 > $tmpfile

delim=" "

total_claim=0
total_success=0
total_fail=0

cut -d"$delim" -f2,3,4 $tmpfile | while read claim success fail ; do
total_claim=$((total_claim+claim))
total_success=$((total_success+success))
total_fail=$((total_fail+fail))


done 
echo $total_claim $total_success $total_fail 


rm $tmpfile
