#!/bin/zsh

outfile="test.png"
tmpfile=`mktemp`

function nop(){}

function segment(){
start=$1
stop=$2
n=$3
label=$4

if ! [ "$start" = "$stop" ]; then

Colors=(red green blue orange purple cyan yellow coral khaki greenyellow)

color=$Colors[$((n%10))]
[ $n -eq 1 ] && color="pink"
pos=$(((start+stop)/(360)))
echo -n 'set object '${n}' circle at screen 0.5,0.5 size '
echo 'screen 0.4 arc ['${start}'   :'${stop}'  ] fillcolor rgb "'${color}'" front'
echo "set label $n \"$label\" at screen cos($pos*pi)*0.45+0.5,  screen sin($pos*pi)*0.45+0.5 center"
fi
}



cat "$@" > $tmpfile

(echo 'set term svg size 600,600

set multiplot

set size square
set style fill solid 1.0 border -1
'
start=0
total=0
n=1
typeset -F total
< $tmpfile | while read label claim success fail; do
total=$((total+fail))
nop $((count++))
done
< $tmpfile | while read label claim success fail; do
end=$((start+(fail/total*360)))
segment ${start%.} ${end%.} $n $label
n=$((n+1))
start=$end
done
echo '
unset border
unset tics
unset key
plot -99 with lines lc rgb "#ffffff"
'
)| gnuplot 


rm $tmpfile
exit
