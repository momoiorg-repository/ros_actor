import sys
from os.path import isfile, expanduser
from importlib import machinery
import yaml

from cm import *
from cm.robots.tb3 import Tb3Interface
import cm.actors # do not erase
from .command import CommandInterpreter

def main():
    args = sys.argv
    m_flag = False
    for a in args:
        if a == 'm': m_flag = True
    command_interpreter = CommandInterpreter()
    init_server(Tb3Interface)
    if m_flag: run_actor('init_navigate')
    command_interpreter.do_command()
    shutdown_server()

if __name__ == '__main__':
    main()