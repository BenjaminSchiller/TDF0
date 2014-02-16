#!/bin/zsh


function plotparts(){
n=${1:-0}
while [ $n -gt 0 ]; do
	echo -n "\"$tmpfile\" using $((n+1)) title \"client $((n--))\" , "
done

}

tmpfile=`mktemp`

cat "$@" > $tmpfile

n=`head -n2 $tmpfile | tail -n1 | wc -w | cut -d "	" -f2`
#set output \"test.svg\"

(echo -n "set terminal svg size 1200,600 fname 'Verdana' fsize 10
set style fill   solid 1.00 border lt -1
set xtics border in scale 1,0.5 nomirror rotate by -45 offset character 0, 0, 0 
set datafile missing '-'
set style data histogram
set style histogram
set style fill solid border -1
set boxwidth 0.75
set key outside;
plot [-1:] [0:] \"$tmpfile\" using 4:xticlabels(1) notitle"
echo
) | gnuplot
rm $tmpfile
exit
