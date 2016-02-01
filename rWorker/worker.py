
import sys
import time
import uuid
import Queue
import multiprocessing
from multiprocessing import Process, Lock

from manager import Manager

class Worker:
	def __init__(self, manager, lock):
		self.lock = lock
		self.running = manager.get_running()
		self.terminating = manager.get_terminating()
		self.workers = manager.get_workers()
		self.status = manager.get_status()
		self.results = manager.get_results()
		self.queue = manager.get_queue()

		self.ID = uuid.uuid4()

		assert not self.workers.has_key(self.ID)
		self.workers.update({ self.ID : 'waiting' })

	def log(self, message):
		self.lock.acquire()
		print '[Worker ' + str(self.ID) + '] ' + message
		self.lock.release()

	def run(self):
		self.log('Waiting for jobs...')
		while self.running.get():
			try:
				job = self.queue.get_nowait()

				assert self.status.has_key(job.ID)
				assert self.results.has_key(job.ID)
				assert self.status.get(job.ID) == 'pending'

				self.log('Job ' + str(job.ID))

				self.workers.update({ self.ID : 'working' })
				self.status.update ({ job.ID : 'running' })
				self.results.update({ job.ID : job.func(job.args) })
				self.status.update ({ job.ID : 'done' })
				self.workers.update({ self.ID : 'waiting' })
			except Queue.Empty:
				pass
		self.workers.pop(self.ID)
		self.log('Shutdown.')

	@staticmethod
	def launch(host, port, num_workers):
		manager = Manager.slave(host, port)
		lock = Lock()
		processes = [ Process(target=Worker.run, args=(Worker(manager, lock),)) for i in range(num_workers) ]

		for process in processes:
			process.start()

		for process in processes:
			process.join()

