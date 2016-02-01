
import time

def sleeper(args):
	time.sleep(1)
	return args

def launch(client):
	print client.map(sleeper, range(10))

