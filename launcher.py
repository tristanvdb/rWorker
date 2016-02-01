
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
		manager = Manager.master(port)
		raw_input('Press any key to quit...')
		manager.terminate()

	elif mode == 'worker':
		Worker.launch(host, port, 2)

	elif mode == 'client':
		client = Client(host, port)
		print client.map(my_func, range(10))

	else:
		assert False

