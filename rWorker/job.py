
import uuid

class Job:
	def __init__(self, func, args, priority):
		self.func = func
		self.args = args
		self.priority = priority

		self.ID = uuid.uuid4()

	def __cmp__(self, other):
		if other == None:
			return False
		return cmp(self.priority, other.priority)

