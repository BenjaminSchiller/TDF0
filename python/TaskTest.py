#!/usr/bin/env python

import unittest
import datetime

from Task import Task


class TaskTest(unittest.TestCase):

	def test_task_init(self):
		# the key 'worker' is allowed, 'bar' not
		task = Task({'worker': 'foo', 'bar': 'baz'})

		# 'worker' is in the task information, 'bar' not
		self.assertEqual(task.get('worker'), 'foo')
		self.assertFalse(task.get('bar'))


	def test_expire_calculation(self):
		format = "%Y-%m-%d %H:%M:%S"
		runBefore = datetime.datetime.today() + datetime.timedelta(hours=1)
		task = Task({'runBefore': runBefore.strftime(format)})
		self.assertFalse(task.isExpired())

		runBefore = runBefore - datetime.timedelta(hours=2)
		task.set('runBefore', runBefore.strftime(format))
		self.assertTrue(task.isExpired())


	def test_start_finish(self):
		task = Task()
		self.assertFalse(task.isStarted())
		task.start('foo')
		self.assertTrue(task.isStarted())

		self.assertFalse(task.isFinished())
		# Don't access the task's attributes like this, use the set- and get-methods!
		# This is done here only for testing purposes!
		task.arg['finished'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		self.assertTrue(task.isFinished())


	def test_requeue_calculation(self):
		task = Task()
		task.start('foo')

		# requeue 1 minute after start
		task.set('timeout', '60000')
		self.assertFalse(task.isTimedOut())

		# requeue 1 minute before start (requeue anyway)
		task.set('timeout', '-60000')
		self.assertTrue(task.isTimedOut())

		# run in one hour
		runAfter = datetime.datetime.now() + datetime.timedelta(hours=1)
		task.set('runAfter', runAfter.strftime("%Y-%m-%d %H:%M:%S"))
		self.assertFalse(task.isTimedOut())

		# run now
		runAfter = datetime.datetime.now() - datetime.timedelta(hours=1)
		task.set('runAfter', runAfter.strftime("%Y-%m-%d %H:%M:%S"))
		self.assertTrue(task.isTimedOut())


if (__name__ == '__main__'):
	unittest.main()
