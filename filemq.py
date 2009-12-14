"""
This module provides message queue based on files in spool directory.
"""

__version__ = '0.2'
__date__ = '2009-11-25 09:04:28 +0300'


import os, re, time
from tempfile import mkstemp
from glob import glob

class FileMQException(Exception):
	"""
	Raised when spool directory does not exist or not directory or could not create message file
	"""
	pass

class Message(object):
	"""
	Message to use with FileMQ
	"""
	def __init__(self, body='', id=None):
		self.body = body
		self.__inner_id = id
	def __repr__(self):
		return repr(self.body)
	def __str__(self):
		return str(self.body)
	def get_inner_id(self):
		return self.__inner_id
	def set_inner_id(self, id):
		self.__inner_id = id

class FileMQ(object):
	"""
	Class represenging message queue
	"""

	def __init__(self, spooldir, msg_suffix='.msg', tmp_suffix='.tmp', wrk_suffix='.wrk', commit_suffix='.msg~', discard_suffix='.dsc'):
		"""
		spooldir: where work on message files is going on
		msg_suffix: message file suffix
		tmp_suffix: temporary message file suffix
		wrk_suffix: suffix for message file being processed
		commit_suffix: suffix for succcessully processed message (if None message file will silently removed)
		discard_suffix: suffix for discarded message (if None message file will be removed)
		"""

		self.msg_suffix = msg_suffix
		self.tmp_suffix = tmp_suffix
		self.wrk_suffix = wrk_suffix
		self.commit_suffix = commit_suffix
		self.discard_suffix = discard_suffix
		self.tmp_re = re.compile('(.+)%s$' % self.tmp_suffix.replace('.', '\.'))
		self.msg_re = re.compile('(.+)%s$' % self.msg_suffix.replace('.', '\.'))
		self.wrk_re = re.compile('(.+)%s$' % self.wrk_suffix.replace('.', '\.'))
		self.spooldir = spooldir

	def chk_spool(self):
		"""
		Check if spool directory exists and is directory
		"""
		if not os.path.exists(self.spooldir):
			try:
				os.mkdir(self.spooldir)
			except:
				raise FileMQException('Spool directory does not exist and could not create it: %s' % self.spooldir)
		if not os.path.isdir(self.spooldir):
			raise FileMQException('%s exists but not directory' % self.spooldir)

	def put(self, msg):
		"""
		Put message to queue.
		If msg.get_inner_id() is set message file name will be formed from this id, otherwise from current time and random mkstemp part
		"""
		try:
			self.chk_spool()
			if not isinstance(msg, Message):
				msg = Message(msg)
			if msg.get_inner_id() is None:
				t = time.time()
				lt = int(t)
				ft = (t - lt)*1e7
				fd, name = mkstemp(suffix=self.tmp_suffix, prefix=time.strftime("%Y%m%d_%H%M%S_%.f_") % ft, dir=self.spooldir)
			else:
				name = self.spooldir + os.sep + msg.get_inner_id() + self.tmp_suffix
				fd = os.open(name, os.O_RDWR|os.O_CREAT|os.O_EXCL, 0640)
			os.write(fd, str(msg))
			os.write(fd, "\n")
			os.close(fd)
			nf = self.tmp_re.match(name).groups()[0] + self.msg_suffix
			os.rename(name, nf)
			return True
		except OSError, e:
			raise FileMQException(str(e))
		except Exception, e:
			print e
			try:
				os.unlink(name)
			except:
				pass
			return False

	def list_queue(self):
		"""
		List all message files to process
		"""
		n = glob("%s/*%s" % (self.spooldir, self.msg_suffix))
		n.sort()
		return n

	def empty(self):
		return len(self.list_queue()) == 0

	def load_msg(self, name):
		"""
		load_msg(name) -> (id, message)

		Load message from named file
		"""
		fn = self.msg_re.match(name).groups()[0]
		id = os.path.basename(fn)
		nf = fn + self.wrk_suffix
		try:
			os.rename(name, nf)
			f = open(nf)
			s = f.read()
			f.close()
			return Message(s, id)
		except Exception, e:
			try:
				os.rename(nf, name)
			except:
				pass
			raise FileMQException(str(e))

	def get(self):
		"""
		get() -> (id, message)
		"""
		names = self.list_queue()
		if names:
			name = names[0]
			return self.load_msg(name)
		return None

	def all(self):
		"""
		Return list of all messages in the queue at the moment
		"""
		names = self.list_queue()
		if names:
			msgs = map(self.load_msg, names)
			return msgs
		return []

	def clear(self):
		map(self.discard, self.all())

	def commit(self, msg):
		"""
		Commit than message defined by msg_id was successfully processed
		"""
		fn = self.spooldir + os.sep + msg.get_inner_id()
		try:
			if self.commit_suffix:
				os.rename(fn + self.wrk_suffix, fn + self.commit_suffix)
			else:
				os.unlink(fn + self.wrk_suffix)
			return True
		except Exception, e:
			raise FileMQException(str(e))

	def rollback(self, msg):
		"""
		Return message to queue
		"""
		fn = self.spooldir + os.sep + msg.get_inner_id()
		try:
			os.rename(fn + self.wrk_suffix, fn + self.msg_suffix)
			return True
		except Exception, e:
			raise FileMQException(str(e))

	def discard(self, msg):
		"""
		Discard message
		"""
		fn = self.spooldir + os.sep + msg.get_inner_id()
		try:
			if self.discard_suffix:
				os.rename(fn + self.wrk_suffix, fn + self.discard_suffix)
			else:
				os.unlink(fn + self.wrk_suffix)
			return True
		except Exception, e:
			raise FileMQException(str(e))

