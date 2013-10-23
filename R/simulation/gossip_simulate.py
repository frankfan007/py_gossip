import os
import sys
from random import randint
from random import choice
import array

def save_to_file(data, outfile):
	outfile.write(','.join(str(node) for node in data))
	outfile.write("\n")
	return outfile

def gen_neigh_list(infile, size):
	neighbour_list = []
	"""
	for i in range(0,37):
		current = infile.readline().strip().split(',')
		neighbour_list.append(map(int, current))
	return neighbour_list
	"""
	for line in infile:
		temp = map(int, line.strip().split(','))
		current_node = []
		for i in range(0,size):
			if (temp[i]):
				current_node.append(i)
		neighbour_list.append(current_node)
	
	return neighbour_list

def file_len(fname):
	with open(fname) as f:
		for i, l in enumerate(f):
			pass
	return i+1


if __name__=="__main__":
	argv = sys.argv[1:]
	file_len = file_len(argv[0])
	infile = open(argv[0],'r')
	outfile = open(argv[0]+"_simulation_result",'w')
	neighbour_list = gen_neigh_list(infile, file_len)
	infile.close()
	size = len(neighbour_list)

	state = array.array('f',(randint(0,1000) for i in xrange(0,size)))
	for i in range(0,int(argv[1])):
		for node in range(0,size):
			dest = choice(neighbour_list[node])
			temp = (state[node] + state[dest]) / 2
			state[node] = temp
			state[dest] = temp
		print ["%0.2f" % j for j in state]
		save_to_file(state, outfile)
	outfile.close()
