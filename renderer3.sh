#!/bin/sh



tmpfile=`mktemp`

function stack(){
echo 
tail -n +2 $1 |\
while read n a b c d e rest; do
echo -n "$n "
echo -n "$a "
echo "$a+$b" | bc -l | tr '\n' ' '
echo "$a+$b+$c " | bc -l | tr '\n' ' '
echo "$a+$b+$c+$d" | bc -l | tr '\n' ' '
echo "$a+$b+$c+$d+$e" | bc -l
done 

}

stack $1 > $tmpfile

echo "set terminal svg size 1200,600 fname 'Verdana' fsize 10
set output \"test.svg\"
set style fill   solid 1.00 border lt -1
set datafile missing '-'
set style data filledcurves x1
plot [1:20] [:5] \
\"$tmpfile\" using 1:6 title \"client 5\",\
\"$tmpfile\" using 1:5 title \"client 4\",\
\"$tmpfile\" using 1:4 title \"client 3\",\
\"$tmpfile\" using 1:3 title \"client 2\",\
\"$tmpfile\" using 1:2 title \"client 1\"\
" | gnuplot
rm -v $tmpfile
exit
