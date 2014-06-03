#!/usr/bin/env python

import datetime

from Task import Task
from TaskError import TaskError


class TaskList(Task):
	def __init__(self, arg={}):
		super().__init__(arg)
		self.set_keys.append('tasks')
		self.get_keys.append('tasks')
		self.set('tasks', [])
		self.path = "tdf.%s.tasklist.%s"


	def addtask(self,task):
		self.get('tasks').append(task)


	def removetask(self,task):
		self.get('tasks').remove(task)


	def save(self, redis, namespace=None, index=None):
		"""
		Saves the task and returns it's index. If a task with the given index exists, it will be updated/overwritten.
		"""
		if (namespace is None):
			if (not self.get('namespace')):
				raise TaskError('Namespace not provided.')
		else:
			self.set('namespace', namespace)

		if (index is None):
			if (not self.get('index')):
				self.set('index',redis.incr("tdf.%s.index" % namespace))
		else:
			self.set('index', index)

		# set task information
		hashKey = self.path % (self.get('namespace'), self.get('index'))
		for key in self.arg:
			if (key not in ['index', 'namespace', 'tasks']):
				redis.hset(hashKey, key, self.arg[key])

		for task in self.get('tasks'):
			task.save(redis,namespace)
			redis.sadd(hashKey+".tasks", task.get('index'))

		return index


	def load(self, redis, namespace, index):
		"""
		Loads a task from the Redis database.
		"""
		self.set('namespace', namespace)
		self.set('index', index)
		hashKey = self.path % (namespace, index)

		for key in self.get_keys:
			value = redis.hget(hashKey, key)
			if (value):
				self.arg[key] = value

		return self
