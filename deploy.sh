#!/bin/bash

usage()
{
echo "Usage: "
echo " $0 TARGETDIR NUMOFCLIENTS"
}

if [ "$(ls -A $1)" ]
then
	echo "Target is not an empty directory!"
	usage
	exit 1
fi

if [ -z "$2" ] || [ $2 -lt 1 ]
then
	echo "NUMOFCLIENTS not a positive integer!"
	usage
	exit 1
fi

export TARGET=${1%/}

mkdir $TARGET/commands

cp java/common/target/{commands.jar,cmd.properties,commands.sh} $TARGET/commands

chmod 755 $TARGET/commands/commands.sh

for command in AddNamespace AddTask AddTaskList DeleteNamespace DeleteTaskList ExportProcessed Requeue Show
do
#	echo -n "Press the ANY-Key to continue..."; read -n 1
	ln -s $(realpath $TARGET/commands/commands.sh) $TARGET/commands/${command}
	echo $(realpath $TARGET/commands/commands.sh) $TARGET/commands/${command}
done

for ((clientnum=0; clientnum<$2; clientnum++))
do
	mkdir $TARGET/client${clientnum}
	cp java/client/target/{client.properties,client.sh,tdf-client.jar} $TARGET/client${clientnum}
	chmod 755 $TARGET/client${clientnum}/client.sh
	sed -i "s/client.id = .*/client.id = \"client-${clientnum}\"/g" $TARGET/client${clientnum}/client.properties
done

mkdir $TARGET/logserver

cp java/logserver/target/tdf-logserver.jar $TARGET/logserver/
