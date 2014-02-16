#!/bin/zsh
source `dirname $0`/uniq.sh

tmpfile=`mktemp`
outfile=`basename ${0} .sh`.`date +%s`
cat "$@" > $tmpfile

delim=","

cut -d"$delim" -f4 $tmpfile | cut -d . -f1 | uniq | sort -h -k 1.7 | while read host ; do
	if [ "$host" ]; then
		echo -n "$host "
		grep $host. $tmpfile | grep -ic Task_Claim | tr -d "\n"
		echo -n " "
		grep $host. $tmpfile | grep -ic Task_Success | tr -d "\n"
		echo -n " "
		grep $host. $tmpfile | grep -ic Task_Fail
	fi
done 


rm $tmpfile
