
import sys
import Queue

from multiprocessing.managers import SyncManager

class Manager(SyncManager):
	_default_port = 6000
	mode = None
	has_instance = False

	def __init__(self, host, port):
		assert Manager.mode != None # make sure the class was initialized
		assert not Manager.has_instance # make sure that Manager is a singleton

		SyncManager.__init__(self, address=(host, port), authkey='rWorker')
		Manager.has_instance = True

		if Manager.mode == 'manager':
			self.start()
		else:
			self.connect()

	@staticmethod
	def class_init(mode):
		assert Manager.mode == None # make sure this method is only called once
		Manager.mode = mode

		if Manager.mode == 'manager':
			queue = Queue.PriorityQueue()
			results = dict()
			Manager.register('get_queue', callable=lambda: queue)
			Manager.register('get_results', callable=lambda: results)
		else:
			Manager.register('get_queue')
			Manager.register('get_results')

