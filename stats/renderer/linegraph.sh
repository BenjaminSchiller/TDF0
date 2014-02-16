#!/bin/zsh

outfile="test.png"
tmpfile=`mktemp`

function nop(){}

cat "$@" > $tmpfile

(echo 'set term svg size 600,400

unset offsets
set style fill solid 1.0 border -1
set offsets 0.5, 0.5, 1, 0
min_y = GPVAL_DATA_Y_MIN
max_y = GPVAL_DATA_Y_MAX
mean(x) = mean_y
fit mean(x) "'$tmpfile'" using 1:2 via mean_y
plot [0:] [0:]  mean_y with lines lc rgb "#0f0f0f" title "mean", "'$tmpfile'" using 1:2 with lines lc rgb "#f01050" title ""
'
)| gnuplot 2>/dev/null


rm $tmpfile
exit
