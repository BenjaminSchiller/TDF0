#!/bin/zsh

outfile="test.png"
tmpfile=`mktemp`
tmpfile2=`mktemp`

function nop(){}

cat "$@" > ${tmpfile}

grep -v '\\' $tmpfile > $tmpfile2

head -n1 $tmpfile | read -A line

size=` echo $line | wc -w | cut -d " " -f1`

(echo 'set term png size 600,'$((size*300+100))'
set multiplot layout '$size',1 downwards
unset offsets
set style fill solid 1.0 border -1
set offsets 0.5, 0.5, 1, 0
#min_y = GPVAL_DATA_Y_MIN
#max_y = GPVAL_DATA_Y_MAX
#mean(x) = mean_y
#fit mean(x) "'$tmpfile'" using 1: via mean_y'
i=1
for t in $line; do
i=$((i+1))
echo 'plot [0:] [0:] "'$tmpfile2'" using 1:'$i' with lines title "'$t'"
'
done
)| gnuplot 2>/dev/null


rm $tmpfile $tmpfile2
exit
