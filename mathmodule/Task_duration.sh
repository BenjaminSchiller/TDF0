#!/bin/zsh
source ./uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat $1 > $tmpfile


delim=','
cut -d"$delim" -f2 $tmpfile | uniq | while read task ; do
echo -n "$host "
grep $task $tmpfile | grep -i Task_Claimed | cut -d"$delim" -f1 | read claimed
grep "^[^$delim]*|[^$delim]*$delim$task" $tmpfile | grep -i Task_Success | cut -d"$delim" -f1 | read completed
echo $task $((completed - claimed))

done | sort


rm $tmpfile
