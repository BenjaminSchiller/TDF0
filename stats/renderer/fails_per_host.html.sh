#!/bin/sh

htmlheader="<html><body>"
htmlfooter="</body></html>"

function handle(){
while [ "$1" ]; do
case "`file --mime-type $1 | cut -d ' ' -f2`" in
	image*) echo "<img src=\"$1\"><br />"
		;;
	"text/plain") table $1
		;;
esac
shift
done

}

function table(){

echo '<table border="1">'

cat $1 |\
while read host started success fail; do
echo "<tr>"
echo -e "\t<td>$host</td><td>$fail</td>"
echo "</tr>"
done
echo '</table><br />'
}
echo $htmlheader 
handle "$@"
echo $htmlfooter
exit
