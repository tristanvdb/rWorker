
import sys
import time

from rWorker.manager import Manager
from rWorker.worker import Worker
from rWorker.client import Client

def my_func(args):
	time.sleep(1)
	return args**2

if __name__ == '__main__':
	mode = sys.argv[1]
	host = 'localhost'
	port = Manager._default_port

	if mode == 'manager':
		Manager.class_init(mode)
		manager = Manager('', port)
		raw_input('Press any key to continue...')
		manager.shutdown()

	elif mode == 'worker':
		Manager.class_init(mode)
		manager = Manager(host, port)
		Worker.launch(manager, 2)

	elif mode == 'client':
		Manager.class_init(mode)
		manager = Manager(host, port)
		client = Client(manager)
		print client.map(my_func, range(10))

	else:
		assert False

