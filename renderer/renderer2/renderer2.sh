#!/bin/zsh


function plotparts(){
n=${1:-0}
while [ $n -gt 0 ]; do
	echo -n "\"$tmpfile\" using $((n+1)) title \"client $((n--))\" , "
done

}

tmpfile=`mktemp`

cat $1 > $tmpfile

n=`head -n2 $tmpfile | tail -n1 | wc -w | cut -d "	" -f2`

(echo -n "set terminal svg size 1200,600 fname 'Verdana' fsize 10
set output \"test.svg\"
set style fill   solid 1.00 border lt -1
set datafile missing '-'
set style data histogram
set style histogram rowstacked
set style fill solid border -1
set boxwidth 0.75
set key outside;
plot [-1:]"
plotparts $((n-1))
echo
) | gnuplot
rm -v $tmpfile
exit
