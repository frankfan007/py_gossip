#!/usr/bin/env python
'''
Title: Tests for Gossip
Author: Niklas Semmler
'''
import unittest
import os
import gossip
import random
import socket
import subprocess

class TestGossip(unittest.TestCase):
    def setUp(self):
        self.experiment_dict = {
            'graph':"GraphXYZ", 'aggregation':"divide_by_two", 'run':"1"
        }
        self.g = gossip
        self.node_name = "Node01"

    def test_load_experiment(self):
        file_experiment = os.tempnam()
        # fails if the experiment file does not exist
        with self.assertRaises(IOError):
            self.g.load_experiment(file_experiment)

        with open(file_experiment, 'w') as f:
            f.write("gargl")
        with self.assertRaises(IndexError):
            self.g.load_experiment(file_experiment)

        # checks if the experiment file is loaded
        with open(file_experiment, 'w') as f:
            f.write("%s,%s,%s" % (
                self.experiment_dict['graph'],
                self.experiment_dict['aggregation'],
                self.experiment_dict['run']
            ))
        experiment = self.g.load_experiment(file_experiment)
        self.assertEquals(self.experiment_dict, experiment)

    def test_read_file_of_neighbours(self):
        file_neighbours = os.tempnam()
        neighbours_dict = {}

        # fails if neighbour file does not exist
        with self.assertRaises(IOError):
            self.g.read_file_of_neighbours(file_neighbours)

        # fails as the format of the file is wrong
        with open(file_neighbours, 'w') as f:
            f.write("gargl")
        with self.assertRaises(ValueError):
            self.g.read_file_of_neighbours(file_neighbours)

        # are the neighbours read?
        for i in xrange(1, 9):
            neighbours_dict['Node' + str(i)] = "192.168.0." + str(i)
        with open(file_neighbours, 'w') as f:
            for key in neighbours_dict:
                f.write("%s,%s\n" % (key, neighbours_dict[key]))
        dict_of_neighbours = self.g.read_file_of_neighbours(file_neighbours)
        self.assertEqual(neighbours_dict, dict_of_neighbours)

    def test_generate_output_file(self):
        folder_output = os.tempnam()

        # checks for fail when no output folder exists
        with self.assertRaises(IOError):
            file_out = self.g.generate_output_file(
                folder_output,
                self.experiment_dict,
                self.node_name
            )

        # folders are created
        os.makedirs(folder_output)
        file_out = self.g.generate_output_file(
            folder_output,
            self.experiment_dict,
            self.node_name
        )
        print file_out
        self.assertTrue(os.path.isfile(file_out))

    def test_save_current_state(self):
        file_results = os.tempnam()

        # make some fake data
        fake_data = []
        start_time = 1050
        for i in xrange(3):
            # the data is made a bit tweaked to make testing more easy
            fake_data.append([str(start_time), str(random.randint(1, 100))])
            start_time += 5

        # fail if the data is not written
        open(file_results, 'w').close()
        self.g.save_current_state(file_results, fake_data)
        expected_data = []
        with open(file_results, 'r') as f:
            f.readline()
            for line in f.readlines():
                expected_data.append(line.strip().split(',')[1:])
        self.assertEqual(fake_data, expected_data)
        #open(file_results, 'w').close()
        #save_current_state(file_results, results)
        #with open(file_results, 'r') as f:
        #    f.read()

    def test_create_destroy_socket(self):
        # create socket
        sock = self.g.create_socket(("127.0.0.1", 5000))
        self.assertIsInstance(sock, socket.socket)

        # address in use
        with self.assertRaises(socket.error):
            sock2 = self.g.create_socket(("127.0.0.1", 5000))

        # socket destroyed and address freeed
        self.g.destroy_socket(sock)
        sock2 = self.g.create_socket(("127.0.0.1", 5000))
        self.assertIsInstance(sock, socket.socket)
        self.g.destroy_socket(sock2)

    #def test_update(self):
    #    raise NotImplementedError

    #def test_get_interface_ip(self):
    #    """ WARNING: OS dependent """
    #    # non existent interface
    #    with self.assertRaises(Exception):
    #        self.g.get_interface_ip_address("en3")
    #    # empty interface name
    #    with self.assertRaises(subprocess.CalledProcessError):
    #        self.g.get_interface_ip_address(" ")

    #    # normal existing interface
    #    iface_ip_address = "127.0.0.1"
    #    return_value = self.g.get_interface_ip_address("lo0")
    #    self.assertEqual(iface_ip_address, return_value)
