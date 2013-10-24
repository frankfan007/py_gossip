import sys
import os
import networkx
import numpy

if __name__=="__main__":
	argv = sys.argv
	graphml = argv[1]
	dest_file = argv[2]
	print argv
	a = networkx.read_graphml(graphml).to_undirected()
	mat = networkx.adjacency_matrix(a)
	mat_array = numpy.asarray(mat)
	numpy.savetxt(dest_file, mat_array, fmt='%d', delimiter=",")
	sys.exit(0)
