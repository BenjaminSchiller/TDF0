#!/bin/zsh
source `dirname $0`/uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat "$@" > $tmpfile

delim=","

cut -d"$delim" -f3 $tmpfile | uniq | while read host ; do
echo -n "$host "
grep $host$delim $tmpfile | grep -ic Task_Claim | tr -d "\n"
echo -n " "
grep $host$delim $tmpfile | grep -ic Task_Success | tr -d "\n"
echo -n " "
grep $host$delim $tmpfile | grep -ic Task_Fail

done | sort 


rm $tmpfile
