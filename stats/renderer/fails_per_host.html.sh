#!/bin/sh

htmlheader="<html><body><table border="1">"
htmlfooter="</table></body></html>"

tmpfile=`mktemp`


cat $1 > $tmpfile


echo $htmlheader 
tail -n +1 $tmpfile |\
while read host started success fail; do
echo "<tr>"
echo -e "\t<td>$host</td><td>$fail</td>"
echo "</tr>"
done
echo $htmlfooter
rm $tmpfile
exit
