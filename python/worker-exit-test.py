#!/usr/bin/env python

from Task import Task
from TaskInterfaceServer import TaskInterfaceServer

tdf = TaskInterfaceServer('localhost', 6379, 15)
tdf.redis.flushall()

task = Task()
task.set('timeout', 5000)
task.set('worker', 'https://dl.dropbox.com/u/1721583/test-worker-2.0.0.zip')

print tdf.addTask('foo', task)
