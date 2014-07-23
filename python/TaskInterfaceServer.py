#!/usr/bin/env python

import redis
import os
import urlparse

from Task import Task
from TaskError import TaskError


class TaskInterfaceServer:

	def __init__(self, host, port, db, password=None):
		if (password is None):
			self.redis = redis.StrictRedis(host=host, port=port, db=db)
		else:
			self.redis = redis.StrictRedis(host=host, port=port, db=db, password=password)


	def getNamespaces(self):
		"""
		Returns a set of names of all namespaces.
		"""
		return self.redis.smembers('tdf.namespaces')


	def addNamespace(self, name):
		"""
		Adds a namespace. Does nothing, if a namespace with the given name already exists.
		"""
		self.redis.sadd('tdf.namespaces', name)
		self.redis.setnx("tdf.%s.index" % name, '-1')


	def deleteNamespace(self, name):
		"""
		Removes a namespace. All tasks in this namespace will be deleted.
		"""
		# remove name from list of namespaces
		self.redis.srem('tdf.namespaces', name)

		# delete namespace index counter
		self.redis.delete("tdf.%s.index" % name)

		# delete all tasks in this namespace
		self.deleteAllTasks(name)

		# delete namespace lists and sets
		self.redis.delete("tdf.%s.queuing" % name)
		self.redis.delete("tdf.%s.running" % name)
		self.redis.delete("tdf.%s.completed" % name)
		self.redis.delete("tdf.%s.processed" % name)


	def deleteAllTasks(self, namespace):
		"""
		Deletes all tasks in the given namespace.
		"""
		deleted = 0

		for taskHashKey in self.redis.keys("tdf.%s.task.*" % namespace):
			index = taskHashKey.rpartition('.')[2]
			self.deleteTask(namespace, index)
			deleted = deleted + 1

		return deleted


	def increaseIndexForNamespace(self, name):
		"""
		Increases the index counter for the given namespace by one and returns it.
		"""
		return self.redis.incr("tdf.%s.index" % name)


	def addTask(self, namespace, task):
		"""
		Adds a task to a namespace and returns it's index.
		If the namespace doesn't exist yet, it will be created.
		The task object needs at least the 'worker' attribute to be valid.
		"""
		if (not task.get('worker')):
			raise TaskError('Task worker information not provided.')

		# create namespace if it does not exist
		if (not self.redis.sismember('tdf.namespaces', namespace)):
			self.addNamespace(namespace)

		# get index for task
		index = self.increaseIndexForNamespace(namespace)

		# save task with new index == creating a new task
		task.save(self.redis, namespace, index)

		# add task to the end of the queuing list
		self.redis.rpush("tdf.%s.queuing" % namespace, index)

		return index


	def deleteTask(self, namespace, index):
		"""
		Deletes a task and removes it from the namespace and all lists / sets.
		"""
		# delete task information
		self.redis.delete("tdf.%s.task.%s" % (namespace, index))

		# delete task from queuing list and running, completed and processed set
		self.redis.lrem("tdf.%s.queuing" % namespace, 0, index)
		self.redis.srem("tdf.%s.running" % namespace, index)
		self.redis.srem("tdf.%s.completed" % namespace, index)
		self.redis.srem("tdf.%s.processed" % namespace, index)


	def getTask(self, namespace, index):
		"""
		Returns a task.
		"""
		hashKey = "tdf.%s.task.%s" % (namespace, index)
		if (self.redis.hget(hashKey, 'worker')):
			return Task().load(self.redis, namespace, index)


	def getQueuedTasks(self, namespace):
		"""
		Returns a list of queued tasks for a namespace.
		"""
		indexes = self.redis.lrange("tdf.%s.queuing" % namespace, 0, -1)
		return map(lambda index: self.getTask(namespace, index), indexes)


	def deleteQueuedTasks(self, namespace):
		"""
		Deletes all tasks that are currently queued
		"""
		deleted = 0

		for index in self.redis.lrange("tdf.%s.queuing" % namespace, 0, -1):
			self.deleteTask(namespace, index)
			deleted = deleted + 1

		return deleted


	def countQueuedTasks(self, namespace):
		"""
		Returns the number of currently queued tasks
		"""
		return self.redis.llen("tdf.%s.queuing" % namespace)


	def getRunningTasks(self, namespace):
		"""
		Returns a list of running tasks for a namespace.
		"""
		indexes = self.redis.smembers("tdf.%s.running" % namespace)
		return map(lambda index: self.getTask(namespace, index), indexes)


	def deleteRunningTasks(self, namespace):
		"""
		Deletes all tasks that are currently running
		"""
		deleted = 0

		for index in self.redis.smembers("tdf.%s.running" % namespace):
			self.deleteTask(namespace, index)
			deleted = deleted + 1

		return deleted


	def countRunningTasks(self, namespace):
		"""
		Returns the number of currently running tasks
		"""
		return self.redis.scard("tdf.%s.running" % namespace)


	def getCompletedTasks(self, namespace):
		"""
		Returns a list of completed tasks for a namespace.
		"""
		indexes = self.redis.smembers("tdf.%s.completed" % namespace)
		return map(lambda index: self.getTask(namespace, index), indexes)


	def deleteCompletedTasks(self, namespace):
		"""
		Deletes all tasks that are completed
		"""
		deleted = 0

		for index in self.redis.smembers("tdf.%s.completed" % namespace):
			self.deleteTask(namespace, index)
			deleted = deleted + 1

		return deleted


	def countCompletedTasks(self, namespace):
		"""
		Returns the number of completed tasks
		"""
		return self.redis.scard("tdf.%s.completed" % namespace)


	def getProcessedTasks(self, namespace):
		"""
		Returns a list of processed tasks for a namespace.
		"""
		indexes = self.redis.smembers("tdf.%s.processed" % namespace)
		return map(lambda index: self.getTask(namespace, index), indexes)


	def deleteProcessedTasks(self, namespace):
		"""
		Deletes all tasks that are processed
		"""
		deleted = 0

		for index in self.redis.smembers("tdf.%s.processed" % namespace):
			self.deleteTask(namespace, index)
			deleted = deleted + 1

		return deleted


	def countProcessedTasks(self, namespace):
		"""
		Returns the number of processed tasks
		"""
		return self.redis.scard("tdf.%s.processed" % namespace)


	def processTask(self, namespace, index):
		"""
		Moves a task from the "finished" to the "processed" set.
		If the task is not in the "finished" set, no operation is performed.
		"""
		self.redis.smove("tdf.%s.completed" % namespace, "tdf.%s.processed" % namespace, index)


	def deleteAllExpiredTasks(self, namespace):
		"""
		Delete all expired tasks from the running set and waiting queue
		"""
		deleted = 0
		# check the running set
		for task in self.getRunningTasks(namespace):
			if task.isExpired():
				deleted = deleted + 1
				self.deleteTask(namespace, task.get('index'))

		# check the waiting queue
		for task in self.getQueuedTasks(namespace):
			if task.isExpired():
				deleted = deleted + 1
				self.deleteTask(namespace, task.get('index'))

		return deleted


	def requeue(self, namespace):
		"""
		Reschedule one specific namespace.
		"""
		# check the running set
		requeued = 0
		for task in self.getRunningTasks(namespace):
			hashKey = "tdf.%s.task.%s" % (namespace, task.get('index'))

			if task.isTimedOut():
				requeued = requeued + 1
				# remove task from the running set
				self.redis.srem("tdf.%s.running" % namespace, task.get('index'));

				# remove client and started information
				self.redis.hdel(hashKey, 'started');
				self.redis.hdel(hashKey, 'client');

				# add task to the front of the queuing list
				self.redis.lpush("tdf.%s.queuing" % namespace, task.get('index'));

		return requeued


	def exportProcessedTasks(self, namespace, tempDir, input=True, output=True, log=True, error=True, information=True):
		"""
		Exports processed tasks and deletes them from the Redis database.
		"""
		exported = 0
		for task in self.getProcessedTasks(namespace):
			exported = exported + 1
			taskUrl = urlparse.urlparse(task.get('worker'))
			workerName = os.path.splitext(os.path.basename(taskUrl.path))[0]

			taskIndexDir = os.path.join(tempDir, task.get('namespace'), workerName, 'tasks', task.get('session'), task.get('index'))
			if not os.path.exists(taskIndexDir):
			    os.makedirs(taskIndexDir)

			if (input):
				inputFile = os.path.join(taskIndexDir, 'input.txt')
				with open(inputFile, 'w') as file:
					file.write(task.get('input') or "")
			if (output):
				outputFile = os.path.join(taskIndexDir, 'output.txt')
				with open(outputFile, 'w') as file:
					file.write(task.get('output') or "")
			if (log):
				logFile = os.path.join(taskIndexDir, 'log.txt')
				with open(logFile, 'w') as file:
					file.write(task.get('log') or "")
			if (error):
				errorFile = os.path.join(taskIndexDir, 'error.txt')
				with open(errorFile, 'w') as file:
					file.write(task.get('error') or "")
			if (information):
				informationFile = os.path.join(taskIndexDir, 'information.txt')
				with open(informationFile, 'w') as file:
					file.write(task.asString() or "")

			self.deleteTask(task.get('namespace'), task.get('index'))

		return exported


	def close(self):
		self.tdf.redis.shutdown()
