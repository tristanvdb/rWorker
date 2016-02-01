
import sys
import Queue

from multiprocessing.managers import SyncManager

class Manager(SyncManager):
	_default_port = 6000
	has_instance = False

	def __init__(self, host, port):
		SyncManager.__init__(self, address=(host, port), authkey='rWorker')
		Manager.has_instance = True

	def terminate(self):
		# TODO notify clients and workers
		self.shutdown()

	@staticmethod
	def master(port):
		assert not Manager.has_instance # make sure that Manager is a singleton

		queue = Queue.PriorityQueue()
		results = dict()

		Manager.register('get_queue', callable=lambda: queue)
		Manager.register('get_results', callable=lambda: results)

		manager = Manager('', port)

		manager.start()

		return manager

	@staticmethod
	def slave(host, port):
		assert not Manager.has_instance # make sure that Manager is a singleton

		Manager.register('get_queue')
		Manager.register('get_results')

		manager = Manager(host, port)

		manager.connect()

		return manager

