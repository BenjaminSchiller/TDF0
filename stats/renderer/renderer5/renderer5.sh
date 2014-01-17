#!/bin/zsh

outfile="test.png"
tmpfile=`mktemp`

function segment(){
start=$1
stop=$2
n=$3

Colors=(red green blue orange purple cyan yellow coral khaki greenyellow)

color=$Colors[$((n%10))]
[ $n -eq 1 ] && color="pink"
echo -n 'set object '${n}' circle at screen 0.5,0.5 size '
echo 'screen 0.45 arc ['${start}'   :'${stop}'  ] fillcolor rgb "'${color}'" front'
}



cat $1 > $tmpfile

(echo 'set term png size 600,600
set output "'${outfile}'"

set multiplot

set size square
set style fill solid 1.0 border -1
'
start=0
total=0
n=1
typeset -F total
for element in `head -n1 $tmpfile`; do
total=$((total+element))
done
for element in `head -n1 $tmpfile`; do
end=$((start+(element/total*360)))
segment ${start%.} ${end%.} $n 
n=$((n+1))
start=$end
done
echo '
unset border
unset tics
unset key
plot x with lines lc rgb "#ffffff"
')| gnuplot 


rm -v $tmpfile
exit
