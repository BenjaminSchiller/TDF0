#!/usr/bin/env python

import datetime

from TaskError import TaskError


class Task:

	def __init__(self, arg={}):
		"""
		Constructor. Task information is provided with a dictionary, only allowed keys are stored in the task's attributes.
		"""
		self.set_keys = "worker input runBefore runAfter timeout waitAfterSuccess waitAfterSetupError waitAfterRunError index namespace session".split()
		self.get_keys = "worker input output log error client started finished runAfter runBefore timeout waitAfterSuccess waitAfterSetupError waitAfterRunError session".split()
		self.arg = dict((k, v) for k, v in arg.items() if k in self.set_keys)
		self.set('session', datetime.datetime.today().strftime("%Y-%m-%d"))
		self.path = "tdf.%s.task.%s"

	def asString(self):
		str = []
		for key in self.arg:
			if (key not in ['input', 'output', 'log', 'error']):
				str.append("%s: %s" % (key, self.arg[key]))
		return "\n".join(str)


	def get(self, key):
		"""
		Returns a task attribute.
		"""
		return self.arg.get(key)


	def set(self, key, value):
		"""
		Sets a task attribute.
		"""
		if (key in self.set_keys):
			self.arg[key] = value


	def save(self, redis, namespace=None, index=None):
		"""
		Saves the task and returns it's index. If a task with the given index exists, it will be updated/overwritten.
		"""
		if (not self.get('worker')):
			raise TaskError('Task worker information not provided.')

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
			if (key not in ['index', 'namespace']):
				redis.hset(hashKey, key, self.arg[key])

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


	def isExpired(self):
		"""
		Decides, if this task should still be executed.
		"""
		if (self.get('runBefore') is None):
			return False

		runBefore = datetime.datetime.strptime(self.get('runBefore'), "%Y-%m-%d %H:%M:%S")
		return runBefore < datetime.datetime.now()


	def isTimedOut(self):
		"""
		Decides if this task should be requeued.
		"""
		if (self.get('started') is None or self.get('timeout') is None):
			return False

		if (self.get('runAfter') is None):
			started = datetime.datetime.strptime(self.get('started'), "%Y-%m-%d %H:%M:%S")
			return (started + datetime.timedelta(milliseconds=int(self.get('timeout')))) < datetime.datetime.now()

		runAfter = datetime.datetime.strptime(self.get('runAfter'), "%Y-%m-%d %H:%M:%S")
		return (runAfter + datetime.timedelta(milliseconds=int(self.get('timeout')))) < datetime.datetime.now()


	def isStarted(self):
		return self.get('started') is not None


	def isFinished(self):
		return self.get('finished') is not None


	def start(self, client):
		self.arg['started'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		self.arg['client'] = client
