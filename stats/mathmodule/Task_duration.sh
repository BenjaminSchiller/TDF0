#!/bin/zsh
source `dirname $0`/uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat $1 > $tmpfile


delim=','
cut -d"${delim}" -f4 $tmpfile | uniq | while read task ; do
#grep $task $tmpfile | grep -i Task_Claimed | cut -d"${delim}" -f1 | read claimed
taskinfo=`grep "${delim}$task\$" $tmpfile | sed 's:$:\\\\n:' `
completed=`echo -e $taskinfo| grep -i Task_Success | cut -d"${delim}" -f1`
claimed=`echo $taskinfo | grep -i starting  | cut -d"${delim}" -f1 `
[ "$completed" ] && echo $task $((completed - claimed))
done | sort


rm $tmpfile
