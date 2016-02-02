
import sys
import time
import Queue

from multiprocessing.managers import SyncManager, Value

class Manager(SyncManager):
	has_instance = False

	def __init__(self, host, port):
		SyncManager.__init__(self, address=(host, port), authkey='rWorker')
		Manager.has_instance = True


	def terminate(self):
		running = self.get_running()
		terminating = self.get_terminating()
		workers = self.get_workers()
		queue = self.get_queue()
		results = self.get_results()

		terminating.set(True)

		print '[Manager] Waiting for all job to finish...'

		while not queue.empty():
			time.sleep(1)

		print '[Manager] Waiting for clients to obtain jobs results...'

		while len(results.keys()) > 0:
			time.sleep(1)

		running.set(False)

		print '[Manager] Waiting for workers to shutdown...'

		while len(workers.keys()) > 0:
			time.sleep(1)

		self.shutdown()

	@staticmethod
	def master(port):
		assert not Manager.has_instance # make sure that Manager is a singleton

		running = Value(bool, True)
		terminating = Value(bool, False)
		workers = dict()
		status = dict()
		results = dict()
		queue = Queue.PriorityQueue()

		Manager.register( 'get_running'     , callable=lambda: running     )
		Manager.register( 'get_terminating' , callable=lambda: terminating )
		Manager.register( 'get_workers'     , callable=lambda: workers     )
		Manager.register( 'get_status'      , callable=lambda: status      )
		Manager.register( 'get_results'     , callable=lambda: results     )
		Manager.register( 'get_queue'       , callable=lambda: queue       )

		manager = Manager('', port)

		manager.start()

		return manager

	@staticmethod
	def slave(host, port):
		assert not Manager.has_instance # make sure that Manager is a singleton

		Manager.register( 'get_running'     )
		Manager.register( 'get_terminating' )
		Manager.register( 'get_workers'     )
		Manager.register( 'get_status'      )
		Manager.register( 'get_results'     )
		Manager.register( 'get_queue'       )

		manager = Manager(host, port)

		manager.connect()

		return manager

