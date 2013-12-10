#!/bin/sh

outfile="test.html"
htmlheader="<html><body><table border="1">"
htmlfooter="</table></body></html>"

tmpfile=`mktemp`


cp $1 $tmpfile


echo $htmlheader >$outfile
tail -n +2 $tmpfile |\
while read n a b c d e rest; do
echo "<tr><td>$n</td><td>$a</td><td>$b</td><td>$c</td><td>$d</td><td>$e</td></tr>"
done>>$outfile
echo $htmlfooter>>$outfile
rm -v $tmpfile
exit
