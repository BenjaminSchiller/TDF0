#!/bin/zsh

outfile=max.dat

function getmax(){
max=0
holder=0
n=0
for i in $@; do
this=$((++n))
[ ${i} -gt $max ] && max=${i} && holder=$this
done
echo $holder
}

function winner(){

tail -n +1 $1 |\
while read n line; do
echo $n `getmax $line`

done

}

tmpfile=`mktemp`


cp $1 $tmpfile

winner $tmpfile > $outfile

rm -v $tmpfile
exit
