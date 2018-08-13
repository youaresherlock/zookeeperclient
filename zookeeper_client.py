# coding: utf-8
from kazoo.client import KazooClient, KazooState
from kazoo import exceptions
import logging

logging.basicConfig()

def my_listener(state):
	if state == KazooState.LOST:
		# 会话丢失
		print 'session lost'
	elif state == KazooState.SUSPENDED:
		# Handle being disconnected from zookeeper
		print 'the connection to Zookeeper cut off'
	else:
		# Handle being connected/reconnected to Zookeeper
		print 'connected to Zookeeper'

def my_func(event):
	''' 使用可以给get或者exists第二个参数指定watch = my_func,即可监控指定的节点'''
	print 'event type is %s' % event.type
	print 'node state is  %s' % event.state
	print 'node path is %s' % event.path

class PyZooConn(object):
	# connection manager class
	def __init__(self):
		self.zk = KazooClient(hosts = 'xxxx.xxxx.xxxx.xxxx:2181')
		self.zk.add_listener(my_listener)
		self.zk.start()

	def get_data(self, param, watch_func = None):
		""" 获取节点的信息 """
		result = self.zk.get(param, watch = watch_func)
		return result

	def get_children_data(self, param):
		lists = []
		if self.zk.exists(param):
			children_list = self.get_children_list(param)
			for each in children_list:
				lists.append(self.get_data(param + '/' + each))
		return lists


	def get_children_list(self, param):
		""" 获取指定节点的所有子节点 """
		if self.zk.exists(param):
			result = self.zk.get_children(param)
			return result

	def set_data(self, param, data):
		try: 
			self.zk.set(param, data)
		except exceptions.NoNodeError as e:
			print '不存在此节点'

	def create_node(self, param, data, overlook = True):
		""" 创建节点，overlook为是否忽略所创建节点所经过节点是否存在，如不存在会默认沿途创建空的节点, 最后一个节点设置为指定值 """
		if self.zk.exists(param):
			print '节点已经存在'
		else:
			if overlook:
				try: 
					self.zk.create(param, data)
				except exceptions.NoNodeError as e:
					print '节点所在路径不存在'
			else:
				self.zk.ensure_path(param)
				self.set_data(param, data)

	def delete_node(self, param):
		""" 删除指定节点，并删除此节点的子节点 """
		if self.zk.exists(param):
			self.zk.delete(param, recursive = False)

	def close(self):
		self.zk.stop()



def main():
	pz = PyZooConn()
	print pz.get_data('/zk_test/child_node/delete_test1', my_func)
	pz.set_data('/zk_test/child_node/delete_test1', '12')
	print pz.get_children_data('/zk_test/child_node')
	pz.create_node('/hello/test/te', 'test', overlook = False)
	print pz.get_data('/hello/test/te')
	pz.close()

"""
测试结果 
connected to Zookeeper
('12', ZnodeStat(czxid=129, mzxid=212, ctime=1534138782714L, mtime=1534141210508L, version=5, cversion=0, aversion=0, ephemeralOwner=0, dataLength=2, numChildren=0, pzxid=129))
event type is CHANGED
node state is  CONNECTED
node path is /zk_test/child_node/delete_test1
[('4', ZnodeStat(czxid=132, mzxid=132, ctime=1534138802390L, mtime=1534138802390L, version=0, cversion=0, aversion=0, ephemeralOwner=0, dataLength=1, numChildren=0, pzxid=132)), ('3', ZnodeStat(czxid=131, mzxid=131, ctime=1534138798494L, mtime=1534138798494L, version=0, cversion=0, aversion=0, ephemeralOwner=0, dataLength=1, numChildren=0, pzxid=131)), ('2', ZnodeStat(czxid=130, mzxid=130, ctime=1534138791190L, mtime=1534138791190L, version=0, cversion=0, aversion=0, ephemeralOwner=0, dataLength=1, numChildren=0, pzxid=130)), ('12', ZnodeStat(czxid=129, mzxid=215, ctime=1534138782714L, mtime=1534141312371L, version=6, cversion=0, aversion=0, ephemeralOwner=0, dataLength=2, numChildren=0, pzxid=129))]
节点已经存在
('test', ZnodeStat(czxid=167, mzxid=177, ctime=1534139613126L, mtime=1534139893861L, version=4, cversion=0, aversion=0, ephemeralOwner=0, dataLength=4, numChildren=0, pzxid=167))
session lost
[Finished in 0.9s]
"""


if __name__ == '__main__':
	main()