#!/bin/sh
[ "$#" -eq 3 ] || exit 1

while read line
do
	echo $line >> $2
done < $1

echo "run log"
echo "run error" 1>&2

exit 0