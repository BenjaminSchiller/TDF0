#!/bin/zsh

outfile="test.png"
tmpfile=`mktemp`

function segment(){
start=$1
stop=$2
n=$3
label=$4

Colors=(red green blue orange purple cyan yellow coral khaki greenyellow)

color=$Colors[$((n%10))]
[ $n -eq 1 ] && color="pink"
echo -n 'set object '${n}' circle at screen 0.5,0.5 size '
echo 'screen 0.45 arc ['${start}'   :'${stop}'  ] fillcolor rgb "'${color}'" front'
pos=$(((start+stop)/2))
echo "set label $n \"$label\" at cos($pos*pi)*1.2, sin($pos*pi)*1.2"
}



cat $1 > $tmpfile

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
done
echo '
unset border
unset tics
unset key
plot x with lines lc rgb "#ffffff"
'
< $tmpfile | while read label claim success fail; do
end=$((start+(fail/total*360)))
segment ${start%.} ${end%.} $n $label
n=$((n+1))
start=$end
done
)| gnuplot 


rm -v $tmpfile
exit
