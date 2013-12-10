#!/bin/sh



tmpfile=`mktemp`

cat $1 > $tmpfile

echo "set terminal svg size 1200,600 fname 'Verdana' fsize 10
set output \"test.svg\"
set style fill   solid 1.00 border lt -1
set datafile missing '-'
set style data histogram
set style histogram rowstacked
set style fill solid border -1
set boxwidth 0.75
plot [-1:23] \
\"$tmpfile\" using 6 title \"client 5\" , \
\"$tmpfile\" using 5 title \"client 4\" , \
\"$tmpfile\" using 4 title \"client 3\" , \
\"$tmpfile\" using 3 title \"client 2\" , \
\"$tmpfile\" using 2 title \"client 1\" \
" | gnuplot
rm -v $tmpfile
exit
