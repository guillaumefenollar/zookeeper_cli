#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This tool uses Kazoo Python Library to access ZooKeeper services 
and provide a set of methods to do basics operations on data

Maintainer : Guillaume Fenollar (guillaume@fenollar.fr)
"""


try:
  from kazoo.client import KazooClient
  from kazoo.exceptions import *
  from os import chdir, access, R_OK
  from os.path import dirname, basename, abspath
  from signal import signal, SIGINT, SIGTERM
  from json import loads, dumps
  import argparse
except ImportError as e:
  print("Error during import of dependencies : {0}".format(e))
  exit(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', '-t', default='zk:2181', \
        help="Zookeeper server host")
    parser.add_argument('--action', '-a', required=True, \
        choices=['get','stats','set','deploy','tree','del','list','status'], \
        help="What to do next")
    parser.add_argument('--znode', '-z', type=str, help="Target ZNode")
    parser.add_argument('--values', '-v', type=check_json, help="Json input")
    parser.add_argument('--input', '-i', type=check_file, help="""
        Input file only used for set (optional) or deploy (mandatory) action. 
        It needs to contain hashes in the form '/zk/key/:file_path', 
        and content of file_path files will be set in the according key.""")
    return parser.parse_args()


def check_json(j):
    """ Check that json values input is correctly formatted """
    dict = loads(j)
    j = dumps(dict, sort_keys=True, indent=4, separators=(',', ': '))
    return j


def check_file(f):
    """ Check that input file is accessible and openable"""
    if not access(f, R_OK):
        print("Can't access or open file {0}".format(f))
        exit(1)
    else:
        chdir(dirname(abspath(f)))
        f = basename(f)
        return f


def check_option(opt, message):
    """ Check option presence, or exit with a message """
    if not opt:
        print(message)
        tear_down(1)


def set_up():
    global zk
    zk = KazooClient(hosts=args.target)
    zk.start(timeout=5)


def tear_down(code):
    """ Called when exiting the program, with appropriate exit code. 
        Ensure ZK connections are closed 
    """
    if 'zk' in globals():
        if zk.connected:
            zk.stop()
            zk.close()
    exit(code)


def check_znode_exists():
    check_option(args.znode, "Option --znode (or -z) is mandatory with the selected action")
    if not zk.exists(args.znode):
        print('ZNode {0} does not exist.'.format(args.znode))
        tear_down(1)


def cmd_list():
    """ List the children of a given znode. Not recursive. """
    check_znode_exists()
    data = zk.get_children(args.znode)

    if not data:
        print('Znode {0} has no child'.format(args.znode))
    else:
        for i in data:
            print(i)


def cmd_status():
    """ Send a healthcheck command to Zookeeper cluster """
    ret = zk.command(cmd='ruok')
    if 'imok' in ret:
        print('OK')
        exit(0)
    else:
        print('WARN - Server replied : {0}'.format(ret))
        exit(1)


def cmd_get():
    """ Get data-only of a given znode """ 
    check_znode_exists()
    output_data = zk.get(args.znode)[0].decode()
    print(output_data.rstrip())


def cmd_stats():
    """ Get stats-only of a given znode """ 
    check_znode_exists()
    output_data = zk.get(args.znode)[1].decode()
    print(output_data.rstrip())


def cmd_del():
    """ Delete a znode and its children """
    check_znode_exists()
    if args.znode == '/':
       print("ROOT cannot be deleted, exiting.")
       tear_down(1)
    print("Deleting znode {0}".format(args.znode))
    zk.delete(args.znode, recursive=True)


def create_and_set():
    try:
        if not zk.exists(args.znode):
            print("Creating znode {0}".format(args.znode))
            zk.ensure_path(args.znode)
        zk.set(args.znode, args.values)
    except ZookeeperError as e:
        print("""Error during creation/insertion of keys/values.
            Full error follows : \n {0}""".format(e)) 

def read_json_from_file(f):
    _j = f.read().rstrip()
    _j = check_json(_j)
    return _j.encode()
    

def cmd_set():
    """ Set a znode to a json value, coming from user input, or read from file. 
        Create it if needed. """
    if not args.input and not args.values:
        print("'Set' action needs option --input or --values positionned.")
        tear_down(1)

    check_option(args.znode, "'Set' action needs --znode (or -z) positionned.")

    if args.input:
        with open(args.input) as f:
            args.value = read_json_from_file(f)

    create_and_set()


def cmd_deploy():
    """ Set a bunch of znodes, given an input mapping file.
    This file contains hashes (one per line), made of :
    /path/to/zk/key:path_to_file
    File (right part) is plain json. Its path can be absolute or relative"""
    check_option(args.input, 'Deploy action needs an input file (-i or --input)')
    with open(args.input) as f:
       for l in f.readlines():
           if not l.startswith('#'):
               key, path = l.rstrip().split(':', 1)
               check_file(path)
       f.seek(0)
       for l in f.readlines():
           if not l.startswith('#'):
               key, path = l.rstrip().split(':', 1)
               with open(path) as p:
                   args.values = read_json_from_file(p)
                   args.znode = key
                   create_and_set()

def cmd_tree():
    """ Print a tree of all znodes, starting from either ROOT, or any znode"""
    if not args.znode:
        args.znode = '/'
    z = ZNode(args.znode, 0)


class ZNode():
    """ Recursively prints all children of a znode """
    def __init__(self, path, level):
        self.path = path
        self.level = level
        self.show_tree_node(self.path)
        try:
            self.children = zk.get_children(path)
            for c in self.children:
                child = ZNode(path + '/' + c,level+1)
        except NoNodeError:
            return

    def show_tree_node(self, c):
        if self.level > 1:
            print("{0}{1}{2} {3}".format('    ' * self.level,
                '└', '─' * self.level, basename(c)))
        elif self.level == 1:
            print("  {0} {1}".format('└───', basename(c)))
        else:
            print("{0} {1}".format('.', basename(c)))


def main():
    global args
    args = parse_args()

    signal(SIGINT, tear_down)
    signal(SIGTERM, tear_down)

    set_up()

    if "get" in args.action:
        cmd_get()
    elif "stats" in args.action:
        cmd_stats()
    elif "set" in args.action:
        cmd_set()
    elif "deploy" in args.action:
        cmd_deploy()
    elif "del" in args.action:
        cmd_del()
    elif "list" in args.action:
        cmd_list()
    elif "tree" in args.action:
        cmd_tree()
    elif "status" in args.action:
        cmd_status()


    tear_down(0)



if __name__ == '__main__':
    main()
