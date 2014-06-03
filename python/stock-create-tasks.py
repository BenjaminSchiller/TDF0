#!/usr/bin/env python

from Task import Task
from TaskInterfaceServer import TaskInterfaceServer
from datetime import datetime, timedelta

tdf = TaskInterfaceServer('localhost', 6379, 14)

for i in range(10):
	runAfter = datetime.now() + timedelta(minutes=i)
	task = Task({
		'worker': 'http://dl.dropbox.com/u/1721583/stock-worker-1.0.0.zip',
		'input': 'http://www.boerse.de/indizes/MDAX/DE0008467416',
		'timeout': 5000,
		'runAfter': runAfter.strftime("%Y-%m-%d %H:%M:%S")
	})
	print tdf.addTask('stock', task)