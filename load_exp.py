import os
import sys
import ConfigParser

def parse_exp_options(Config, section):
    sec_dict = {}
    options = Config.options(section)
    for option in options:
        try:
            sec_dict[option] = Config.get(section, option)
        except Exception, e:
            raise e
    return sec_dict

def parse_exp_section(Config):
    sections = Config.sections()
    exp_dict = {}
    for section in sections:
        try:
            exp_dict[section] = parse_exp_options(Config, section)
            if exp_dict[section] == -1:
                logger.error("section miss")
        except Exception, e:
            logger.error("ill exp file")
            raise e
    return exp_dict

def parse_exp_file(exp_file):
    Config = ConfigParser.ConfigParser()
    exp_dict = {}
    Config.read(exp_file)
    exp_dict = parse_exp_section(Config)
    return exp_dict

if __name__ == "__main__":
    exp_dict = {}
    try:
        exp_dict = parse_exp_file("exp.ini")
    except Exception, e:
        raise e

    graph_name = exp_dict['Graph']['name']
    graph_links = exp_dict['Graph']['links']
    operation_name = exp_dict['Operation']['name']
    run = exp_dict['Run']['cycles']

    print "Graph is %s, with %s links, operation is %s" % (graph_name, graph_links, operation_name)
    print "Experiment will run until %sth cycle" % (run)