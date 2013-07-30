#!/bin/sh
[ "$#" -eq 3 ] || exit 1

# get numbers array from input file
source $1

SUM=0

# add numbers
for n in ${NUMBERS[@]}
do
	SUM=$(($SUM + $n))
done

# write sum to output file
echo $SUM > $2

exit 0