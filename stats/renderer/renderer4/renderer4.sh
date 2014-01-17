#!/bin/sh

outfile="test.html"
htmlheader="<html><body><table border="1">"
htmlfooter="</table></body></html>"

tmpfile=`mktemp`


cp $1 $tmpfile


echo $htmlheader >$outfile
tail -n +2 $tmpfile |\
while read line; do
echo "<tr>"
for i in `echo $line`; echo "\t<td>$i</td>"
echo "</tr>"
done>>$outfile
echo $htmlfooter>>$outfile
rm -v $tmpfile
exit
