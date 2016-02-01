
import sys
import time
import Queue
import multiprocessing
from multiprocessing import Process

from manager import Manager

class Worker:
	def __init__(self, host, port, ID):
		self.manager = Manager.slave(host, port)
		self.queue = self.manager.get_queue()
		self.results = self.manager.get_results()
		self.ID = ID

	def run(self):
		while True:
			job = self.queue.get()
			print '[Worker #' + str(self.ID) + '] Job ' + str(job.ID)
			self.results.update({ job.ID : job.func(job.args) })

	@staticmethod
	def launch(manager, num_workers):
		processes = [ Process(target=Worker.run, args=(Worker(manager, i),)) for i in range(num_workers) ]

		for process in processes:
			process.start()

		for process in processes:
			process.join()

