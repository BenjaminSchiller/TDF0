#!/bin/zsh

outfile="test.png"
tmpfile=`mktemp`

function segment(){
start=$1
stop=$2
color=$3
n=$4

echo -n 'set object '${n}' circle at screen 0.5,0.5 size '
echo 'screen 0.45 arc ['${start}'   :'${stop}'  ] fillcolor rgb "'${color}'" front'
}

cp $1 $tmpfile
a=.2
b=.2
c=.1
d=.15
e=.35


(echo 'set term png size 600,600
set output "'${outfile}'"

set multiplot

set size square
set style fill solid 1.0 border -1
'
segment $((0*(360))) $((a*(360))) "red" 1
segment $((a*(360))) $(((a+b)*(360))) "blue" 2
segment $(((a+b)*(360))) $(((a+b+c)*(360))) "orange" 3
segment $(((a+b+c)*(360))) $(((a+b+c+d)*(360))) "green" 4
segment $(((a+b+c+d)*(360))) $(((a+b+c+d+e)*(360))) "purple" 5
echo '
unset border
unset tics
unset key
plot x with lines lc rgb "#ffffff"
')| gnuplot 


rm -v $tmpfile
exit
