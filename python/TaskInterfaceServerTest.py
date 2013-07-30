#!/usr/bin/env python

import unittest
import datetime
import tempfile
import os

from TaskInterfaceServer import TaskInterfaceServer
from Task import Task
from TaskError import TaskError


class TaskInterfaceServerTest(unittest.TestCase):

	format = "%Y-%m-%d %H:%M:%S"

	def setUp(self):
		# connect to Redis database
		self.tdf = TaskInterfaceServer('localhost', 6379, 15)

		# clear the test database
		self.tdf.redis.flushall()


	def test_get_missing_task(self):
		self.assertFalse(self.tdf.getTask('asd', -1))


	def test_namesapces(self):
		# the initial list of namespaces should be empty
		self.assertFalse(self.tdf.getNamespaces())

		# add two namespaces there should be two namespaces in the list
		self.tdf.addNamespace('foo')
		self.tdf.addNamespace('bar')
		self.assertTrue(len(self.tdf.getNamespaces()) == 2)

		# change the index counter
		self.assertTrue(self.tdf.redis.get('tdf.foo.index'), '-1')
		self.tdf.redis.set('tdf.foo.index', '1')

		# try to add the namespace "foo" again it should not be added
		self.tdf.addNamespace('foo')
		self.assertTrue(len(self.tdf.getNamespaces()) == 2)

		# index counter should still be "1"
		self.assertTrue(self.tdf.redis.get('tdf.foo.index'), '1')

		# add task to "bar"
		index = self.tdf.addTask('bar', Task({'worker': 'foo'}))
		self.assertTrue(self.tdf.countQueuedTasks('bar') == 1)

		# remove the namespace "bar", now there should be only one namespace left
		self.tdf.deleteNamespace('bar')
		self.assertTrue(len(self.tdf.getNamespaces()) == 1)

		# the task added to "bar" should not exist anymore
		self.assertTrue(self.tdf.countQueuedTasks('bar') == 0)


	def test_add_and_remove_task(self):
		# create task object
		task = Task()
		runBefore = datetime.datetime.today() + datetime.timedelta(hours=1)
		# time attributes need to be a string with format yyyy-MM-dd hh:mm:ss
		task.set('runBefore', runBefore.strftime(self.format))

		# try to add without worker
		with self.assertRaises(TaskError):
			self.tdf.addTask('foo', task)

		# add task to queue
		task.set('worker', 'worker-uri')
		index = self.tdf.addTask('foo', task)
		self.assertEquals(self.tdf.getTask('foo', index).get('worker'), 'worker-uri')

		# move task to running
		index = self.tdf.redis.lpop('tdf.foo.queuing')
		self.tdf.redis.sadd('tdf.foo.running', index)
		self.assertTrue(self.tdf.countRunningTasks('foo') == 1)

		# move task to completed
		self.tdf.redis.smove('tdf.foo.running', 'tdf.foo.completed', index)
		self.assertTrue(self.tdf.countCompletedTasks('foo') == 1)

		# process task
		self.tdf.processTask('foo', index)
		self.assertTrue(self.tdf.countCompletedTasks('foo') == 0)
		self.assertTrue(self.tdf.countProcessedTasks('foo') == 1)

		# remove task
		self.assertTrue(self.tdf.deleteAllTasks('foo') == 1)
		self.assertTrue(self.tdf.countProcessedTasks('foo') == 0)
		self.assertFalse(self.tdf.getTask('foo', index))


	def test_delete_queued_tasks(self):
		# create task
		task = Task({'worker': 'worker-uri'})

		# add it to queued list
		index = self.tdf.addTask('foo', task)
		self.assertEqual(self.tdf.countQueuedTasks('foo'), 1)

		# delete all queued tasks, list should be empty and task deleted
		self.tdf.deleteQueuedTasks('foo')
		self.assertEqual(self.tdf.countQueuedTasks('foo'), 0)


	def test_delete_running_tasks(self):
		# create task
		task = Task({'worker': 'worker-uri'})
		index = self.tdf.addTask('foo', task)

		# move task to running
		self.tdf.redis.lpop('tdf.foo.queuing')
		self.tdf.redis.sadd('tdf.foo.running', index)
		self.assertEqual(self.tdf.countRunningTasks('foo'), 1)

		# delete all running tasks, list should be empty and task deleted
		self.tdf.deleteRunningTasks('foo')
		self.assertEqual(self.tdf.countRunningTasks('foo'), 0)


	def test_delete_completed_tasks(self):
		# create task
		task = Task({'worker': 'worker-uri'})
		index = self.tdf.addTask('foo', task)

		# move task to completed
		self.tdf.redis.lpop('tdf.foo.queuing')
		self.tdf.redis.sadd('tdf.foo.completed', index)
		self.assertEqual(self.tdf.countCompletedTasks('foo'), 1)

		# delete all completed tasks, list should be empty and task deleted
		self.tdf.deleteCompletedTasks('foo')
		self.assertEqual(self.tdf.countCompletedTasks('foo'), 0)


	def test_delete_processed_tasks(self):
		# create task
		task = Task({'worker': 'worker-uri'})
		index = self.tdf.addTask('foo', task)

		# move task to processed
		self.tdf.redis.lpop('tdf.foo.queuing')
		self.tdf.redis.sadd('tdf.foo.processed', index)
		self.assertEqual(self.tdf.countProcessedTasks('foo'), 1)

		# delete all processed tasks, list should be empty and task deleted
		self.tdf.deleteProcessedTasks('foo')
		self.assertEqual(self.tdf.countProcessedTasks('foo'), 0)


	def test_rescheduling(self):
		# create task and "start" it
		task = Task({'worker': 'worker-uri', 'timeout': '100000'})

		# add task to queue
		index = self.tdf.addTask('foo', task)

		# move task to running
		self.tdf.redis.lpop('tdf.foo.queuing')
		self.tdf.redis.sadd('tdf.foo.running', index)
		self.tdf.redis.hset("tdf.foo.task.%s" % index, 'started', datetime.datetime.now().strftime(self.format))

		# requeue, task should still be in the running set
		self.tdf.requeue('foo')
		self.assertEqual(self.tdf.countRunningTasks('foo'), 1)

		# change requeue time
		self.tdf.redis.hset("tdf.foo.task.%s" % index, 'timeout', -10000)

		# requeue, task should be in the queuing list
		self.tdf.requeue('foo')
		self.assertEqual(self.tdf.countRunningTasks('foo'), 0)
		self.assertEqual(self.tdf.countQueuedTasks('foo'), 1)

		# change expire timestamp
		runBefore = datetime.datetime.now() - datetime.timedelta(minutes=10)
		self.tdf.redis.hset("tdf.foo.task.%s" % index, 'runBefore', runBefore.strftime(self.format))

		# requeue (that includes checking for expired tasks)
		# task should still be in the queuing list
		self.tdf.deleteAllExpiredTasks('foo')
		self.assertEqual(self.tdf.countQueuedTasks('foo'), 0)


	def test_expire_running_tasks(self):
		# create task and "start" it
		runBefore = datetime.datetime.now() + datetime.timedelta(minutes=10)
		task = Task({'worker': 'worker-uri', 'runBefore': runBefore.strftime(self.format)})

		# add task to queue
		index = self.tdf.addTask('foo', task)

		# move task to running
		self.tdf.redis.lpop('tdf.foo.queuing')
		self.tdf.redis.sadd('tdf.foo.running', index)
		self.tdf.redis.hset("tdf.foo.task.%s" % index, 'started', datetime.datetime.now().strftime(self.format));

		# requeue (that includes checking for expired tasks)
		# task should still be in the running set
		self.tdf.requeue('foo')
		self.assertEqual(self.tdf.countRunningTasks('foo'), 1)

		# change expire timestamp
		runBefore = datetime.datetime.now() - datetime.timedelta(minutes=10)
		self.tdf.redis.hset("tdf.foo.task.%s" % index, 'runBefore', runBefore.strftime(self.format))

		# requeue, running set should be empty
		self.tdf.deleteAllExpiredTasks('foo')
		self.assertEqual(self.tdf.countRunningTasks('foo'), 0)


	def test_export_processed_tasks(self):
		tempDir = tempfile.mkdtemp()

		# create task
		task = Task({'worker': 'http://dl.dropbox.com/u/1721583/test-worker-1.0.0.zip', 'input': 'input'})
		index = self.tdf.addTask('foo', task)

		# set task processed
		self.tdf.redis.sadd('tdf.foo.processed', index)
		self.assertTrue(self.tdf.countProcessedTasks('foo') == 1)

		# export
		self.tdf.exportProcessedTasks('foo', tempDir)
		self.assertTrue(self.tdf.countProcessedTasks('foo') == 0)

		taskIndexDir = os.path.join(tempDir, 'foo', 'test-worker-1.0.0', 'tasks', datetime.datetime.today().strftime("%Y-%m-%d"), index.__str__())
		self.assertEquals(len(os.listdir(taskIndexDir)), 5)


if (__name__ == '__main__'):
	unittest.main()
