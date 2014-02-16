#!/bin/zsh 
source `dirname $0`/uniq.sh
source `dirname $0`/fails_over_time.sh.inc

tmpdir=`mktemp -d`
outfile=`basename ${0} .sh`.`date +%s`
#while [ "$2" ]; do
#shift
#done
cat "$@" > $tmpdir/root

for name in `cut -d, -f4 $tmpdir/root | cut -d. -f1 | uniq | tr "\n" " "`; 
do
	echo $name>$tmpdir/$name.dat
		grep ,$name. $tmpdir/root > $tmpdir/$name.log
	fails_over_time $tmpdir/$name.log 60000 |cut -d " " -f2 >>$tmpdir/$name.dat
done
paste -d , $tmpdir/*.dat | sed -e 's:,,:,0,:g' -e 's:,,:,0,:g' -e 's:,: :g' > $tmpdir/total
(echo '\\' ; seq -w 1 $((`wc -l $tmpdir/total| cut -d " " -f1` - 1 )) ) | paste -d" " - $tmpdir/total
rm -r $tmpdir 2>/dev/null
