
import time
import socket

from .manager import Manager
from .job import Job

class Client:
	def __init__(self, manager):
		self.running = manager.get_running()
		self.terminating = manager.get_terminating()
		self.status = manager.get_status()
		self.results = manager.get_results()
		self.queue = manager.get_queue()

	def submit(self, func, args, priority=0):
		try:
			if self.terminating.get() or not self.running.get():
				print 'Manager is shuting down...'
				exit(1)

			job = Job(func, args, priority)
			assert not self.status.has_key(job.ID)
			assert not self.results.has_key(job.ID)
			self.status.update({ job.ID : 'pending' })
			self.results.update({ job.ID : None })
			self.queue.put(job)
			return job.ID
		except socket.error:
			print 'Problem while synchronizing with manager...'
			exit(1)

	def query(self, job_id):
		try:
			if not self.running.get():
				print 'Manager is shuting down...'
				exit(1)

			assert self.status.has_key(job_id)
			assert self.results.has_key(job_id)
			if self.status.get(job_id) == 'done':
				return ( self.status.pop(job_id) , self.results.pop(job_id) )
			else:
				return ( self.status.get(job_id) , None )

		except socket.error:
			print 'Problem while synchronizing with manager...'
			exit(1)

	def map(self, mapper, args_list, priority=0):
		wait = set()
		IDs = list()
		results = dict()
		for args in args_list:
			ID = self.submit(mapper,args,priority)
			wait.add(ID)
			IDs.append(ID)

		while len(wait) > 0:
			for ID in wait:
				results.update({ ID : self.query(ID) })
			wait = set(filter(lambda ID: results[ID][0] != 'done', wait))
			time.sleep(1)

		res = list()
		for ID in IDs:
			assert ID in results
			res.append(results[ID][1])
		return res

