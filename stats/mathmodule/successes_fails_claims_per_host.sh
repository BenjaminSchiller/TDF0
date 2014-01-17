#!/bin/zsh
source uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat $1 > $tmpfile

delim=","

cut -d"$delim" -f2 $tmpfile | uniq | while read host ; do
echo -n "$host "
grep $host$delim $tmpfile | grep -ic Task_Claim | tr -d "\n"
echo -n " "
grep $host$delim $tmpfile | grep -ic Task_Success | tr -d "\n"
echo -n " "
grep $host$delim $tmpfile | grep -ic Task_Fail

done | sort 


rm $tmpfile
