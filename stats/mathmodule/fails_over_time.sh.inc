function fails_over_time(){

logfile=$1
interval=$2
host=$3

tmpfile=`mktemp`

cat $logfile > $tmpfile

head -n1 $tmpfile | cut -d \) -f2 | cut -d , -f1 | read firstevent

start=$((firstevent/interval))

a=""
typeset -A a

index=0
grep -i task_fail $tmpfile | while read event; do

echo $event | cut -d \) -f2 | cut -d , -f1 | read time
index=$(((time/interval)-start))
a[$index]=$((a[$index]+1))

done

stop=$index
index=0

while [ $index -le $stop  ]; do
echo $index $((a[$((index++))]))
done
rm $tmpfile

}
