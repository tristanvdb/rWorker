
import os
import sys
import json
import time
import multiprocessing

from rWorker.manager import Manager
from rWorker.worker import Worker
from rWorker.client import Client

import paramiko

paramiko.util.log_to_file('.paramiko-ssh.log')

ssh_config = paramiko.SSHConfig()
user_config_file = os.path.expanduser("~/.ssh/config")
if os.path.exists(user_config_file):
	with open(user_config_file) as f:
		ssh_config.parse(f)

def ssh_connect(hostname, username=None, port=None):
	client = paramiko.SSHClient()
	client._policy = paramiko.WarningPolicy()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	client.load_system_host_keys()

	user_config = ssh_config.lookup(hostname)

	cfg = { 'hostname': hostname }

	if username != None:
		cfg.update({ 'username' : username })
	elif 'user' in user_config:
		cfg.update({ 'username' : user_config['user'] })

	if port != None:
		cfg.update({ 'port' : port })
	elif 'port' in user_config:
		cfg.update({ 'port' : user_config['port'] })

	if 'proxycommand' in user_config:
		cfg.update({ 'sock' : paramiko.ProxyCommand(user_config['proxycommand']) }) 

	client.connect(**cfg)

	return client

def usage(name):
	print 'Usage:'
	print '\t' + name + ' --mode=manager --module=module [--host=localhost] [--port=6000] [--workers=XXX]'
	print '\t\t* host: hostname or IP to be used by workers to connect to this manager'
	print '\t\t* workers: JSON file with a list of dict of the form:'
	print '\t\t\t{ \'hostname\' : \'XXX\' , \'path\' : \'/path/to/rWorker\' , \'username\' : \'XXX\' , \'ssh-port\' : 22 , \'num\' : 0 } (\'username\' , \'port\' and \'num\' are optional)'
	print '\t' + name + ' --mode=worker  --module=module [--host=localhost] [--port=6000] [--num=(system default)]'
	print '\t\t* host: hostname or IP of manager'
	print '\t' + name + ' --mode=client  --module=module [--host=localhost] [--port=6000]'
	print '\t\t* host: hostname or IP of manager'

if __name__ == '__main__':
	mode = None
	module = None
	workers = None
	host = 'localhost'
	port = 6000
	num = multiprocessing.cpu_count()

	#### Parse arguments ####

	i = 1
	while i < len(sys.argv):
		if sys.argv[i].startswith('--mode'):
			mode = sys.argv[i].split('=')[1]
		elif sys.argv[i].startswith('--module'):
			module = sys.argv[i].split('=')[1]
		elif sys.argv[i].startswith('--host'):
			host = sys.argv[i].split('=')[1]
		elif sys.argv[i].startswith('--workers'):
			workers = sys.argv[i].split('=')[1]
		elif sys.argv[i].startswith('--port'):
			port = int(sys.argv[i].split('=')[1])
		elif sys.argv[i].startswith('--num'):
			num = int(sys.argv[i].split('=')[1])
		else:
			usage(sys.argv[0])
			exit(1)
		i += 1

	if mode == None or module == None:
		usage(sys.argv[0])
		exit(1)
	if mode != 'manager' and mode != 'worker' and mode != 'client':
		usage(sys.argv[0])
		exit(1)

	#### Process arguments ####

	if mode == 'manager' and workers == None:
		workers = list()
	elif mode == 'manager':
		if not os.path.isfile(workers):
			print 'Error: ' + workers + ' is not a valid file.'
			exit(2)

		with open(workers) as F:
			try:
				workers = json.load(F)
			except:
				print 'Error: cannot parse ' + workers + ' as a JSON file.'
				exit(2)

		if not isinstance(workers, list):
			print 'Error: expected a list of workers.'
			exit(1)
		error = False
		for worker in workers:
			if not isinstance(worker, dict) or not 'hostname' in worker or not 'path' in worker:
				print 'Error: invalid worker description: ' + str(worker)
				error = True
		if error:
			exit(2)

	try:
		pymodule = __import__(module)
	except ImportError:
		print 'Error: module ' + module + ' not found'
		exit(2)

	#### Start ####

	if mode == 'manager':
		manager = Manager.master(port)

		for worker in workers:
			hostname = worker['hostname']
			username = worker['username'] if 'username' in worker else None
			port     = worker['ssh-port'] if 'ssh-port' in worker else None
			command  = 'python ' + worker['path'] + '/launcher.py --mode=worker --module=' + module + ' --host=' + host + ' --port=' + str(port)
			if 'num' in worker:
				command += ' --num=' + str(worker['num'])

			print hostname + ': command: ' + command

			worker.update({ 'ssh' : ssh_connect(hostname=hostname, username=username, port=port) })
			worker.update({ 'inouterr' : worker['ssh'].exec_command(command) })

		raw_input('Return to quit...')
		manager.terminate()

		for worker in workers:
			worker['inouterr'][0].close()

			for line in worker['inouterr'][1].read().splitlines():
				print worker['hostname'] + ': out: ' + line
			worker['inouterr'][1].close()

			for line in worker['inouterr'][2].read().splitlines():
				print worker['hostname'] + ': err: ' + line
			worker['inouterr'][2].close()

			worker['ssh'].close()

			del worker['ssh']
			del worker['inouterr']			

	if mode == 'worker':
		Worker.launch(host, port, 2)

	if mode == 'client':
		manager = Manager.slave(host, port)
		client = Client(manager)
		pymodule.launch(client)

	#### End ####

