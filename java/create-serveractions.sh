#!/bin/bash

tmpdir=`mktemp -d`
manifestfile=`mktemp`
srcdir=`pwd`
trap "rm -rf $manifestfile $tmpdir" EXIT
jars="argo-3.7.jar commons-cli-20040117.000000.jar commons-io-2.4.jar gson-2.2.4.jar hamcrest-all-1.3.jar jedis-1.5.2.jar joda-time-2.3.jar json-20090211.jar junit-4.11.jar zip4j_1.3.1.jar"
actions="AddNamespace AddTask DeleteNamespace DeleteTask DeleteTaskList ExportProcessed Requeue Show"
manifestheader='Manifest-Version: 1.0\nMain-Class:'
cd $tmpdir

cp -r $srcdir/common/bin/de $tmpdir

for jar in $jars; do
	jar xf $srcdir/$jar
done
rm -r META-INF LICENSE.txt

for action in $actions; do
 echo -e $manifestheader de.tuda.p2p.tdf.cmd.$action > $manifestfile
 jar cfm $srcdir/$action.jar $manifestfile *
done
