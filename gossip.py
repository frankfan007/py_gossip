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
 TODO: check that no unrelated traffic blocks the connections
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
import ConfigParser
import argparse
import signal


class GossipEpoch(object):
    """ managing the epochs of the gossip algorithm """
    def __init__(self, start_time, max_epoch, epoch_dur):
        self._start_time = start_time
        self._max_epoch = max_epoch
        self._epoch_duration = epoch_dur

    def start(self):
        """ wait till the experiment starts """
        time_to_wait = self._start_time - time.time()
        if time_to_wait > 0:
            time.sleep(time_to_wait)
        else:
            raise Exception("Not started yet start time is already over... \
                Exiting...")

    def next_epoch(self):
        """ proceed to the next epoch (blocking) """
        self._epoch += 1
        # sleep till next epoch
        next_cycle = self._epoch * self._epoch_duration + self._start_time
        sleep_time = next_cycle - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            raise Exception("already over the next epoch's start time")

    def last_epoch_reached(self):
        """ check for end of experiment
        """
        if self._epoch < self._max_epoch:
            return True
        else:
            return False

    def stop(self):
        """ stop experiment now """
        self._epoch = self._max_epoch

    @property
    def curr_epoch(self):
        return self._epoch

class GossipState(object):
    """ managing the state of the gossip algorithm """

    def __init__(self, initial_state, gossip_epoch):
        self._state = initial_state
        self._gossip_epoch = gossip_epoch
        self._state_history = []
        self._lock = threading.Lock()

    def get_and_acquire(self):
        """ get current state and acquire lock """
        self._lock.acquire()
        return self._state

    def update_and_release(self, new_state, epoch):
        """ update state, add to history and release lock """
        def aggregate(state, new_state):
            """ Aggregate states """
            return (state + new_state) / 2
        self._state = aggregate(self._state, new_state)
        self._state_history.append(
            [self._gossip_epoch.curr_epoch, time.time(), self._state]
        )
        self._lock.release()
        return self._state

    @property
    def history(self):
        return self._state_history

class GossipThread(threading.Thread):
    """ basic thread for gossip algorithm """


class ActiveThread(GossipThread):
    def __init__(self, sock, config, logger, gossip_state, gossip_epoch,
            dict_of_neighbours):
        self.sock = sock
        self.config = config
        self.logger = logger
        self.gossip_state = gossip_state
        self.gossip_epoch = gossip_epoch
        self.dict_of_neighs = dict_of_neighbours
        super(GossipThread, self).__init__()

    def run(self):
        """ wait for nodes asking for the state and reply
        """
        epoch_info = self.config.items('epochs')
        error_count, error_limit = 0, self.config.get('threads', 'max_error')
        self.sock.timeout(self.config.get('network', 'socket_timeout'))
        epoch = 0
        while not self.gossip_epoch.last_epoch_reached:
            try:
                neighbour_key = random.choice(self.dict_of_neighs.keys())
                self.logger.debug("Connecting to %s",
                    dict_of_neighbours[neighbour_key])
                connection = self.sock.connect((
                    dict_of_neighbours[neighbour_key],
                    self.config.get('network', 'recv_port')
                ))
                msg_send = json.dumps(
                    self.gossip_state.get_and_acquire()
                )
                self.sock.send(msg_send)
                msg_recv = connection.recv(
                    self.config.get('network', 'socket_buffer_size')
                )
                new_state = json.loads(msg_recv)
                self.gossip_state.update_and_release(new_state)

                self.gossip_epoch.next_epoch()
            except:
                if error_count < error_limit:
                    self.logger.exception("passive thread had an error!")
                else:
                    self.logger.error("passive thread had 10 errors!")
                    print "FAILED"
                    self.gossip_epoch.stop()
        self.sock.close()

class PassiveThread(GossipThread):
    def run(self):
        """ wait for nodes asking for the state and reply
        """
        error_count, error_limit = 0, self.config.get('threads', 'max_error')
        self.sock.timeout(self.config.get('network', 'socket_timeout'))
        while not self.gossip_epoch.last_epoch_reached:
            try:
                connection, address = self.sock.accept()
                self.logger.debug("Connected to %s at port %s",
                    address[0], address[1])
                msg_recv = connection.recv(
                    self.config.get('network', 'socket_buffer_size')
                )
                msg_send = json.dumps(
                    self.gossip_state.get_and_acquire()
                )
                connection.send(msg_send)
                connection.close()

                new_state = json.loads(msg_recv)
                self.gossip_state.update_and_release(new_state)
            except:
                if error_count < error_limit:
                    self.logger.exception("passive thread had an error!")
                else:
                    self.logger.error("passive thread had 10 errors!")
                    print "FAILED"
                    self.gossip_epoch.stop()
        self.sock.close()

class BaseDaemon(object):
    def __init__(self):
        self.logger = logging.getLogger()
        signal.signal(signal.SIGINT, self.signal_handler)

    def parse_arguments(self, descr, args):
        """ defines arguments of program """
        parser = argparse
        parser = argparse.ArgumentParser(description=descr)
        for arg in args:
            parser.add_argument(arg[0], **arg[1])
        return parser.parse_args()

    def parse_config(self, configuration_path):
        """ parse configuration file """
        configx = ConfigParser.RawConfigParser()
        configx.read(configuration_path)
        return configx

    def create_logger(self, formatter, path_to_file):
        """ create logging object """
        logx = logging.getLogger()
        file_handler = logging.FileHandler(path_to_file, mode='w')
        file_handler.setFormatter(
            logging.Formatter(formatter)
        )
        logx.addHandler(file_handler)
        logx.setLevel(logging.DEBUG)
        return logx

    def signal_handler(self, signum, stackframe):
        """ handle all signals """
        self.logger.warn("Got signal %s!", signum)
        print "got signal %s!" % signum

class GossipDaemon(BaseDaemon):
    def __init__(self):
        options = [
            ('-f', {'dest':"configpath", 'type':str, 'required':True,
            'help':"locate the config file"}),
            ('-t', {'dest':"start_time", 'type':float, 'required':True,
            'help':"start time for the experiment"}),
            ('-s', {'dest':"state", 'type':float, 'required':True,
            'help':"initial state"})
        ]
        args = self.parse_arguments("Gossip aggregator agent", options)
        self.config = self.parse_config(args.configpath)
        self.threads = {}
        self.gepoch = GossipEpoch(
            args.start_time,
            self.config.get('epochs', 'max'),
            self.config.get('epochs', 'duration')
        )
        self.gstate = GossipState(args.state, self.gepoch)

    def create_socket(self, address):
        """ create socket
        """
        try:
            sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        except socket.error:
            self.logger.error('Could not create socket')
            raise
        try:
            sock.bind(address)
        except socket.error:
            self.logger.error('Could not bind to socket')
            raise
        return sock

    def generate_output_files(self, root_folder, sub_folder, node_name):
        """ return output files for writing data and logs
        """
        print "Create folder %s" % os.path.join(root_folder, sub_folder)

        def mkdir_p(folders):
            """ unix 'mkdir -p' synonym """
            path = folders.lstrip(os.sep).split(os.sep)
            for i in xrange(1, len(path)+1):
                sub_path = os.sep + os.sep.join(path[:i])
                if not os.path.isdir(sub_path):
                    if not os.path.exists(sub_path):
                        os.mkdir(sub_path)
                    else:
                        os.remove(sub_path)
                        os.mkdir(sub_path)

        if not os.path.isdir(root_folder):
            raise IOError

        if not os.path.isdir(sub_folder):
            mkdir_p(os.path.join(root_folder, sub_folder))

        output_file = os.path.join(root_folder, sub_folder, node_name + '.csv')
        log_file = os.path.join(root_folder, sub_folder, node_name + '.log')
        open(output_file, 'w').close()
        open(log_file, 'w').close()
        return output_file, log_file

    def read_file_of_neighbours(self, file_list_of_neighbours):
        """ get local neighbours
            <Neighbour>,<IP>
        """
        self.logger.debug("reading neigbour file at %s",
            file_list_of_neighbours
        )
        dict_of_neighbours = {}
        separator = re.compile(r',\s?')
        try:
            with open(file_list_of_neighbours, 'r') as f:
                for line in f.readlines():
                    try:
                        neighbour_id, neighbour_ip = separator.split(
                            line.strip()
                        )
                    except ValueError:
                        self.logger.error("neighbour file is ill-formatted")
                        raise
                    else:
                        dict_of_neighbours[neighbour_id] = neighbour_ip
        except IOError:
            self.logger.error("neighbour file could not be read")
            raise
        else:
            return dict_of_neighbours

    def get_interface_ip_address(self, iface):
        """ get IP address of interface """
        if sys.platform == 'darwin':
            inet_regex = \
                r'inet ([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})'
            command = ["ifconfig", iface]
        else:
            inet_regex = \
                r'inet ([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})/[0-9]{1,3}'
            command = ["ip", "addr", "show", "dev", iface]

        regex = re.compile(inet_regex)
        try:
            output = subprocess.check_output(command)
        except subprocess.CalledProcessError:
            self.logger.error("Command was not successfully excecuted")
            raise

        iface_ip_list = re.findall(regex, output)
        if len(iface_ip_list) == 1:
            return iface_ip_list[0]
        elif len(iface_ip_list) > 1:
            self.logger.warn("more than one ip on interface: %s", iface_ip_list)
            return iface_ip_list[0]
        else:
            self.logger.error("no ip found on interface. Exiting...")
            raise Exception

    def store_results(self, file_results):
        """ write the current to a file for later analysis
            In: (<time>,<state>)
            Out: <epoch>,<time>,<state>
        """
        self.logger.debug("saving state history")
        if not os.path.isfile(file_results):
            self.logger.warn("output file does not exist... should have been created \
    during setup. Trying to recreate....")
            try:
                open(file_results, 'w').close()
            except (OSError, IOError):
                self.logger.error("Recreating file failed")
                raise

        try:
            with open(file_results, 'w') as f:
                f.write("counter,time,state\n")
                for line in xrange(len(self.gstate.history)):
                    f.write("%s\n" % ','.join(line))
        except (OSError, IOError):
            self.logger.error("Could not write state history.")
            raise

    def main(self):
        def experiment_path():
            return os.path.join(
                    self.config.get('experiment', 'aggregation'),
                    self.config.get('experiment', 'graph'),
                    self.config.get('experiment', 'run')
            )

        node_name = socket.gethostname()

        try:
            output_file, log_file = self.generate_output_files(
                self.config.get('paths', 'root_folder'),
                experiment_path(),
                node_name
            )
        except:
            print "Could not generate output files :( Exiting..."
            sys.exit(1)

        self.logger = self.create_logger(
            self.config.get('logging', 'format'), log_file
        )

        try:
            node_ip = self.get_interface_ip_address(self.config.get(
                'network', 'node_interface'
            ))
        except:
            self.logger.exception("Could not get ip address of interface %s",
                self.config.get('network', 'node_interface')
            )
            sys.exit(1)

        dict_of_neighbours = self.read_file_of_neighbours(
            self.config.get('paths', 'list_of_neighbours_file')
        )
        thread_parameters = (
            self.config, self.logger, self.gstate, self.gepoch
        )
        addresses = {
            'active':(node_ip, int(self.config.get('network', 'recv_port'))),
            'passive':(node_ip, int(self.config.get('network', 'send_port'))),
        }
        self.prepare_threads(addresses, thread_parameters)
        self.run_threads(dict_of_neighbours)
        self.store_results(output_file)

    def prepare_threads(self, addresses, thread_parameters):
        """ initialize threads """
        socks = {}
        try:
            for name, addr in addresses.iteritems():
                socks[name] = self.create_socket(addr)
        except socket.error:
            self.logger.error("Could not setup network")
            sys.exit(1)

        passive_thread = PassiveThread(socks['passive'], *thread_parameters)
        active_thread = ActiveThread(socks['active'], *thread_parameters)
        self.threads['passive'] = passive_thread
        self.threads['active'] = active_thread

    def run_threads(self, dict_of_neighbours):
        """ Starting active and passive thread """
        self.threads['passive'].start()
        self.threads['active'].start(dict_of_neighbours)
        self.threads['active'].join()
        self.threads['passive'].join()

if __name__ == '__main__':
    gossip_daemon = GossipDaemon()
    gossip_daemon.main()
    print "Success! :)"
    sys.exit(0)
