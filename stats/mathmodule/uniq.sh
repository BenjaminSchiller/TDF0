function uniq(){
read array
while read line; do

if [ "$line" ] && ! echo $array|grep -q "^$line\$"; then
array="$array
$line"
echo $line
fi
done

}
