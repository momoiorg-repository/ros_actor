from os.path import isfile, expanduser
from importlib import machinery
import yaml

from . import run_actor, check_actor

DB_FILE = expanduser('~/.cm_db')

class CommandInterpreter:
    def __init__(self) -> None:
        self.load_db()
    
    def load_db(self):
        if isfile(DB_FILE):
            with open(DB_FILE) as f:
                self.value_table = yaml.safe_load(f)
        else:
            self.value_table = {}
            self.dump_db()

    def dump_db(self):
        with open(DB_FILE, 'w') as f:
            yaml.dump(self.value_table, f)
    
    def load_command(self, module, file):
        loader = machinery.SourceFileLoader(module, file)
        loader.load_module()
        
    def run_command(self, name):
        self.load_db()
        db = self.value_table.get(name)
        if not db:
            print(f'{name}: not known')
            return
        args = db['value']
        com = db['com']
        ret = run_actor(com, *args)
        print(ret)   
    
    def r_command(self, *args):
        if len(args) == 0:
            value = 1
        else:
            value = float(args[0])
        adj = [value, 0, 0, 0]
        run_actor('adjust_joint', *adj)

    def l_command(self, *args):
        if len(args) == 0:
            value = 1
        else:
            value = float(args[0])
        adj = [-value, 0, 0, 0]
        run_actor('adjust_joint', *adj)

    def list_command(self): 
        self.load_db()
        for k,v in self.value_table.items():
            print(f"{k}: ({v['com']}) {v['value']}")
    
    def regular_command(self, com, elms):
        if not check_actor(com):
            self.run_command(com)
            return
        args = []
        for e in elms:
            args.append(eval(e))
        ret = run_actor(com, *args)
        print(ret)

    def do_command(self):
        while True:
            line = input('$ ')
            elms = line.split()
            if len(elms) <= 0: continue
            com = elms.pop(0)
            if com == 'exit': break
            elif com == 'load':
                self.load_command(elms[0], elms[1])
                continue
            elif com == 'run':
                name = elms.pop(0)
                self.run_command(name)
                continue
            elif com == 'list':
                self.list_command()
                continue
            elif com == 'l':
                self.l_command(*elms)
                continue
            elif com == 'r':
                self.r_command(*elms)
                continue
            else:
                self.regular_command(com, elms)
