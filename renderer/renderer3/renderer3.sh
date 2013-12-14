#!/bin/sh



tmpfile=`mktemp`

function plotparts(){
n=${1:-0}
while [ $n -gt 0 ]; do
	echo -n "\"$tmpfile\" using 1:$((n+1)) title \"client $((n--))\" , "
done
	
}

function sumup(){
total=0
for this in `echo $*`; do
#this=$i
total=$((total+this))
echo -n "$total "
done
echo
}
	

function stack(){
echo 
tail -n +2 $1 |\
while read n line; do
echo -n "$n "
sumup $line[@]
done 

}

stack $1 > $tmpfile

n=`head -n2 $tmpfile | tail -n1 | wc -w | cut -d "	" -f2`
datasetcount=`wc -l $tmpfile | cut -d " " -f1`
echo "$datasetcount"
(echo -n "set terminal svg size $(((datasetcount*30)+50)),$((n*20)) fname 'Verdana' fsize 10
set output \"test.svg\"
set style fill   solid 1.00 border lt -1
set datafile missing '-'
set style data filledcurves x1
set key outside;
plot [1:] "
plotparts $((n-1))
) | gnuplot
rm -v $tmpfile
exit
