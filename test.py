
import sys
import time

from rWorker.manager import Manager
from rWorker.worker import Worker
from rWorker.client import Client

from tests.sleep import launch

if __name__ == '__main__':
	mode = sys.argv[1]
	host = 'localhost'
	port = 6000

	if mode == 'manager':
		manager = Manager.master(port)
		raw_input('Press any key to quit...')
		manager.terminate()

	elif mode == 'worker':
		Worker.launch(host, port, 2)

	elif mode == 'client':
		manager = Manager.slave(host, port)
		client = Client(manager)
		launch(client)

	else:
		assert False

