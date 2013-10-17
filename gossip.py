#!/usr/bin/env python
"""
Title: Gossip Agent
Author: Niklas Semmler
Description: Gossip algorithm as taken from:

M. Jelasity, A. Montresor, and O. Babaoglu, "Gossip-based aggregation in large
 dynamic networks," ACM Transactions on Computer Systems (TOCS), vol. 23,
 no. 3, pp. 219-252, 2005.

Pseudo Code:
ActiveThread
    for each consecutive *delta* time units at randomly picked time; do
        q <- GET_NEIGBOUR()
        send state_p to q
        state_q <- receive(q)
        state_p <- UPDATE(state_p, state_q)

PassiveThread
    while true:
    state_q <- receive(*)
    send state_p to sender(state_q)
    state_p <- UPDATE(state_p, state_q)


Structure:
    Input files:
    neighbour_list => <name_of_host>,<ip_address_of_host>
    experiment => <graph>,<aggregation>,<Run ID>,<start time>,<num of epochs>

    Output files:
    <path>/<aggregation>/<graph>/<run>/<node>.csv => <epoch>,<time>,<state>
    <path>/<aggregation>/<graph>/<run>/<node>.log


Next steps:
 TODO: state_history should not be a list but epoch, timestamps, state value
 TODO: start at given time
 TODO: change active thread so to start not always at the beginning of the
  epoch
 TODO: how is 'convergence' defined? number of epochs? when does the experiment
  start? when end? / clean thread exit!
 TODO: check that no unrelated traffic blocks the connections
 TODO: log to same folder as csv
 TODO: change load_experiment to really load all values needed
"""

import subprocess
import logging
import sys
import os
import re
import random
import socket
import threading
import json
import time

def create_logger(formatter):
    """ create logging object """
    logger = logging.getLogger('gossip daemon')
    out_file = logging.FileHandler('/var/tmp/myapp.log') # TODO: wrong path!
    out_file.setFormatter(formatter)
    logger.addHandler(out_file)
    logger.setLevel(logging.WARNING)
    return logger

def get_interface_ip_address(iface):
    """ get IP address of interface """
    regexp = re.compile(
        r'inet ([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})/[0-9]{1,3}'
        # for testing on Mac
        #r'inet ([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})'
    )
    try:
        output = subprocess.check_output(
            ["ip", "addr", "show", "dev", iface]
            # for testing on Mac
            #["ifconfig", iface]
        )
    except subprocess.CalledProcessError:
        logger.error("Command was not successfully excecuted")
        raise

    iface_ip_list = re.findall(regexp, output)
    if len(iface_ip_list) == 1:
        return iface_ip_list[0]
    elif len(iface_ip_list) > 1:
        logger.warn("more than one ip on interface: %s", iface_ip_list)
        return iface_ip_list[0]
    else:
        logger.error("no ip found on interface. Exiting...")
        raise Exception

""" --- Constants --- """
FORMAT = "%(asctime)-15s [%(level)s] %(message)s"
FILE_LIST_OF_NEIGHBOURS = "/etc/gossip_neighbors"
FILE_EXPERIMENTAL_SETUP = "/hosthome/new_home/gossip_experiment"
FOLDER_RESULTS = "/hosthome/new_home/results/"
SOCKET_BUFFER_SIZE = 1024
SOCKET_TIMEOUT = 5
RECV_PORT = 5001
SEND_PORT = 5002
MAX_ERRORS_PER_THREAD = 10
NODE_INTERFACE = "lo:1"

""" --- Constants (need to be defined via function) --- """
NODE_NAME = socket.gethostname()
logger = create_logger(FORMAT)

""" --- global variables ---- """
threads = []
lock_state = threading.Lock()
state = None
state_history = []
converged = False

def mkdir_p(folders):
    """ unix 'mkdir -p' synonym """
    logger.debug("Create folders %s", folders)
    path = folders.lstrip(os.sep).split(os.sep)
    for i in xrange(1, len(path)+1):
        sub_path = os.sep + os.sep.join(path[:i])
        print sub_path
        if not os.path.isdir(sub_path):
            if not os.path.exists(sub_path):
                os.mkdir(sub_path)
            else:
                os.remove(sub_path)
                os.mkdir(sub_path)

def read_file_of_neighbours(file_list_of_neighbours):
    """ get local neighbours
        <Neighbour>,<IP>
    """
    logger.debug("reading neigbour file at %s", file_list_of_neighbours)
    dict_of_neighbours = {}
    separator = re.compile(r',\s?')
    try:
        with open(file_list_of_neighbours, 'r') as f:
            for line in f.readlines():
                try:
                    neighbour_id, neighbour_ip = separator.split(line.strip())
                except ValueError:
                    logger.error("neighbour file is ill-formatted")
                    raise
                else:
                    dict_of_neighbours[neighbour_id] = neighbour_ip
    except IOError:
        logger.error("neighbour file could not be read")
        raise
    else:
        return dict_of_neighbours

def load_experiment(file_experimental_setup):
    """ define graph name and aggregation function to be used
        <Graph>,<Aggregation>,<Run>
    """
    logger.debug("reading experiment setup file at %s", file_experimental_setup)
    experiment = {}
    separator = re.compile(r',\s?')
    try:
        with open (file_experimental_setup, 'r') as f:
            """ --- JIANNAN: this should be done via configuration parser --- """
            tuplex = separator.split(f.readline().strip())
            try:
                experiment['graph'] = tuplex[0]
                experiment['aggregation'] = tuplex[1]
                experiment['run'] = tuplex[2]
                """ --- END --- """
            except IndexError:
                logger.error("experiment file is ill formatted") 
                raise
    except IOError:
        logger.error("experiment file could not be read")
        raise
    else:
        return experiment

def generate_output_file(folder_results, experiment, node_name):
    """ return output folder for writing data and logs
        <path>/<aggregation>/<graph>/<run>/<node>.csv
    """
    logger.debug("generating output file")
    if not os.path.isdir(folder_results):
        logger.error("root output folder %s does not exist", folder_results)
        raise IOError

    sub_folder_results = os.sep.join([
        folder_results,
        experiment['aggregation'],
        experiment['graph'],
        experiment['run']]
    )
    if not os.path.isdir(sub_folder_results):
        mkdir_p(sub_folder_results)

    output_file = os.sep.join([sub_folder_results, node_name])
    try:
        open(output_file, 'w').close()
    except (IOError, OSError):
        logger.error("Could not create output file!")
        raise
    else:
        return output_file

def save_current_state(file_results, state_history):
    """ write the current to a file for later analysis
        In: (<time>,<state>)
        Out: <epoch>,<time>,<state>
    """
    logger.debug("saving state history")
    if not os.path.isfile(file_results):
        logger.warn("output file does not exist... should have been created \
during setup. Trying to recreate....")
        try:
            open(file_results, 'w').close()
        except (OSError, IOError):
            logger.error("Recreating file failed")
            raise

    try:
        with open(file_results, 'w') as f:
            f.write("counter,time,state\n")
            for i in xrange(len(state_history)):
                f.write("%s,%s,%s\n" % (
                    i, state_history[i][0], state_history[i][1]
                ))
    except (OSError, IOError):
        logger.error("Could not write state history.")
        raise

def update(state, new_state):
    """ Aggregate states """
    state = (state+new_state) / 2
    return state

def create_socket(address):
    """ create socket
    """
    try:
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    except socket.error:
        logger.error('Could not create socket')
        raise
    try:
        sock.bind(address)
    except socket.error:
        logger.error('Could not bind to socket')
        raise
    return sock

def destroy_socket(sock):
    """ close socket
    """
    sock.close()

def is_converged(state_history):
    # show if state_history is converged
    raise NotImplementedError

def exit_program(exit_code, output_file):
    for threadx in threads:
        threadx.join()
    save_current_state(output_file, state_history)
    sys.exit(exit_code)

class gossipThread(threading.Thread):
    def __init__(self, sock, dict_neighs, exp):
        self.sock = sock
        self.dict_of_neighbours = dict_neighs
        self.experiment = exp

    def exit_thread(self, exit_code):
        # TODO: fix
        global converged
        converged = True

class activeThread(gossipThread):
    def run(self):
        """ wait for nodes asking for the state and reply
        """
        error_count, error_limit = 0, MAX_ERRORS_PER_THREAD
        self.sock.timeout(SOCKET_TIMEOUT)
        global converged
        global state
        global state_history
        while not converged: # execute once every epoch
            try:
                neighbour_key = random.choice(self.dict_of_neighbours.keys())
                logger.debug("Connecting to %s", dict_of_neighbours[neighbour_key])
                self.sock.connect((dict_of_neighbours[neighbour_key], RECV_PORT))

                # ----- LOCK REGION BEGIN -----
                lock_state.acquire()
                msg_send = json.dumps(state)
                self.sock.send(msg_send)
                msg_recv = self.sock.recv(SOCKET_BUFFER_SIZE)
                new_state = json.loads(msg_recv)

                # state updating
                state = update(state, new_state)
                state_history.append(state)

                # sleep till next epoch
                converged = is_converged(state_history) # TODO: define converged
                lock_state.release()
                # ----- LOCK REGION END -----
            except:
                if error_count < error_limit:
                    logger.exception("passive thread had an error!")
                else:
                    logger.error("passive thread had 10 errors!")
                    self.exit_thread(1)
        destroy_socket(self.sock)
        self.exit_thread(0)

class passiveThread(gossipThread):
    def run(self):
        """ wait for nodes asking for the state and reply
        """
        error_count, error_limit = 0, MAX_ERRORS_PER_THREAD
        self.sock.timeout(SOCKET_TIMEOUT)
        global state
        global state_history
        while not converged: # always listen
            try:
                # connection handling
                connection, address = self.sock.accept()
                logger.debug("Connected to %s at port %s", address[0], address[1])
                # ----- LOCK REGION BEGIN -----
                lock_state.acquire()
                msg_send = json.dumps(state)
                msg_recv = connection.recv(SOCKET_BUFFER_SIZE)
                new_state = json.loads(msg_recv)
                connection.send(msg_send)
                connection.close()

                # state updating
                state = update(state, new_state)
                state_history.append(state)
                lock_state.release()
                # ----- LOCK REGION END-----
            except:
                if error_count < error_limit:
                    logger.exception("passive thread had an error!")
                else:
                    logger.error("passive thread had 10 errors!")
                    self.exit_thread(1)
        destroy_socket(self.sock)

if "__name__" == "__main__":
    try:
        node_ip = get_interface_ip_address(NODE_INTERFACE)
    except Exception:
        logger.exception()
        sys.exit(1)

    experiment = load_experiment(FILE_EXPERIMENTAL_SETUP)
    dict_of_neighbours = read_file_of_neighbours(FILE_LIST_OF_NEIGHBOURS)
    output_file = generate_output_file(FOLDER_RESULTS, experiment, NODE_NAME)

    passive_sock = create_socket((node_ip, RECV_PORT))
    active_sock = create_socket((node_ip, SEND_PORT))

    passive_thread = passiveThread(
        passive_sock, dict_of_neighbours, experiment, output_file
    )
    passive_thread.start()
    threads.append(passive_thread)

    active_thread = activeThread(
        active_sock, dict_of_neighbours, experiment, output_file
    )
    active_thread.start()
    threads.append(active_thread)
