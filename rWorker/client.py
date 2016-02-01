
import time
from .manager import Manager
from .job import Job

class Client:
	def __init__(self, manager):
		self.queue = manager.get_queue()
		self.results = manager.get_results()

	def submit(self, func, args, priority=0):
		job = Job(func, args, priority)
		self.queue.put(job)
		return job.ID

	def query(self, job_id):
		if self.results.has_key(job_id):
			return self.results.pop(job_id)
		else:
			return None

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
			wait = set(filter(lambda ID: results[ID] == None, wait))
			time.sleep(1)
		res = list()
		for ID in IDs:
			assert ID in results
			res.append(results[ID])
		return res

