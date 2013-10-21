import os
import sys
from random import randint
from random import choice
import array

def save_to_file(data, outfile):
	outfile.write(','.join(str(node) for node in data))
	outfile.write("\n")
	return outfile

def gen_neigh_list(infile):
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
		for i in range(0,8):
			if (temp[i]):
				current_node.append(i)
		neighbour_list.append(current_node)
	
	return neighbour_list


if __name__=="__main__":
	infile = open('geant','r')
	outfile = open('result_geant','w')
	neighbour_list = gen_neigh_list(infile)
	infile.close()
	size = len(neighbour_list)
	print neighbour_list

	state = array.array('f',(randint(0,1000) for i in xrange(0,size)))
	for i in range(0,30):
		for node in range(0,size):
			dest = choice(neighbour_list[node])
			temp = (state[node] + state[dest]) / 2
			state[node] = temp
			state[dest] = temp
		save_to_file(state, outfile)
	outfile.close()
