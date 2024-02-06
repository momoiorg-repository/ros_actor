from enum import Enum
from dataclasses import dataclass
from threading import Thread, Event, Semaphore
import time
import yaml
import inspect
from dataclasses import dataclass
from typing import Any

import rclpy
from rclpy.action import ActionServer, ActionClient
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup

from pytwb import lib_main

from cm_interfaces.action import ComplexMotion

#############################################################
# core data structures
#############################################################
@dataclass
class ActorDefinition:
    impl: callable
    name: str
    is_async: bool = False
    is_multi: bool = False
    close: callable = None

# id for 4 types of actor invocation
class TransactionType(Enum):
    SYNC = 1
    ASYNC = 2
    ITERATOR = 3
    MULTI_ASYNC = 4

@dataclass
class ActorTransaction:
    actor: ActorDefinition
    tran_type: TransactionType
    args: list = None
    keys: dict = None
    callback: callable = None
    close: callable = None
    abort: callable = None
    is_timed: bool = False
    time: float = 0.0
    start: float = 0.0
    is_extern: bool = False
    
# counter for statistics
num_task = 0
max_task = 0
verbose = False
    
#############################################################
#   API
#############################################################
# 'actor' decorator   
def actor(*a_arg):
    if len(a_arg) < 1: raise Exception('actor argument error')
    if callable(a_arg[0]):
        func = a_arg[0]
        def dfunc(self, *dt_arg, **dd_arg):
            if self.reg_mode:
                self.register_actor(
                    func.__name__,
                    lambda *f_arg, **k_arg: func(self, *f_arg, **k_arg),
                    'sync'
                )
            else:
                return func(self, *dt_arg, **dd_arg)
        return dfunc
    else:
        name = a_arg[0]
        def _dfunc(func):
            def dfunc(self, *dt_arg, **dd_arg):
                if self.reg_mode:
                    self.register_actor(
                        name,
                        lambda *f_arg, **k_arg: func(self, *f_arg, **k_arg),
                        *a_arg[1:]
                    )
                else:
                    return func(self, *dt_arg, **dd_arg)
            return dfunc
        return _dfunc

def register_bt(name):
    actor = do_register_bt(name)
    root_subsystem.set_value_local(name, actor)

def register_subsystem(name, ss):
    root_subsystem.add_subsystem(name, ss)

def get_value(name):
    return root_subsystem.get_value(name)

def set_value(name, value):
    root_subsystem.set_value(name, value)

def check_actor(name):
    actor = root_subsystem.get_value(name)
    return (actor and isinstance(actor, ActorDefinition)) 

def run_actor(name, *l_args, **k_args):
    actor = get_value(name)
    if not (actor and isinstance(actor, ActorDefinition)):
        raise Exception(f'run_actor({name}) type error')
    at = ActorTransaction(
        actor,
        TransactionType.SYNC,
        l_args,
        k_args,
        is_extern=True
    )
    return do_run_actor(at)

def run_actor_async(name, callback, *l_args, **k_args):
    actor = get_value(name)
    if not (actor and isinstance(actor, ActorDefinition)): raise Exception('run_actor type error')
    at = ActorTransaction(
        actor,
        TransactionType.ASYNC,
        l_args,
        k_args,
        callback=callback
    )
    do_run_actor(at)

def init_spin(node, max_task_param=10):
    global actor_executor, max_task
    max_task = max_task_param
    actor_executor = rclpy.executors.MultiThreadedExecutor(max_task+2)
    actor_executor.add_node(node)
    executor_thread = Thread(target=actor_executor.spin, daemon=True, args=())
    executor_thread.start()
    node.create_rate(1.0).sleep()

w_tran = 0    
def actor_watchdog():
    global w_tran
    now = time.time()
    for t in active_tran:
        if not t.is_timed: continue
        elasp = now - t.start
        if elasp >= t.time:
            t.abort(t)
    w_tran += 1
    if w_tran % 10 == 0:
        dump_tran()

def init_server(app_init, verbose_param=False):
    global bt_engine, actor_node, cm_server, verbose
    verbose = verbose_param
    rclpy.init()
    node = Node('actor_node')
    set_value('node', node)
    actor_node = node
    actor_node.create_timer(1, actor_watchdog)
    set_value('callback_group', ReentrantCallbackGroup())
    lib_main.initialize()
    bt_engine = BTEngine()
    app_init(actor_node)
    cm_server = CMActionServer(node)
    bt_engine.run_bt()

def shutdown_server():
    rclpy.shutdown()

def get_actor_tran():
    return active_tran

#############################################################
#   Actor Space
#      SubSystem and SubNet are the containers for actors
#############################################################

# Actor instance
@dataclass
class ContextEntity:
    value: Any
    registrant: 'SubSystem'

# base class for SubSystem and SubNet
class ActorBase:
    def __init__(self) -> None:
        self.reg_mode = True
        for x in inspect.getmembers(self, inspect.ismethod):
            name = x[0]
            if name.startswith('__'): continue
            inst = x[1]
            if inst.__func__.__name__ != 'dfunc': continue
            inst()
        self.reg_mode = False
    
    def get_value(self, name):
        if '.' in name: # absolute path
            current = root_subsystem
            elms = name.split('.')
            for e in elms[:-1]:
                current = current.child.get(e)
                if not current: raise Exception(f'get_value: name error({name})')
            return current.context.get(elms[-1])
        else: # relative path
            res = root_subsystem.context.get(name)
            if res: return res.value
            current = self.subsystem
            while True:
                res = current.context.get(name)
                if res: return res.value
                current = current.parent
                if not current: return None
    
    def set_value(self, name, value):
        global root_subsystem
        root_subsystem.context[name] = ContextEntity(value, self)
        
    def set_value_local(self, name, value):
        ent = ContextEntity(value, self)
        current = self.subsystem
        current.context[name] = ent
        while True:
            current = current.parent
            if not current: return
            if name in current.context:
                v = current.context[name]
                if not v: return # already conflicted
                if v.registrant != self: # new conflict
                    break
            current.context[name] = ent
        while True:
            current.context[name] = None
            current = current.parent
            if not current: return
    
    def register_actor(self, name, value, mode, *args):
        args = list(args)
        ad = None
        mode_list = mode.split('_')
        if 'sync' in mode_list: # synchronous call
            ad = ActorDefinition(
                value,
                name
            )
        elif 'async' in mode_list:
            ad = ActorDefinition(
                value,
                name,
                is_async=True
            )
        if not ad:
            raise Exception('actor definition mode error')
        self.set_value_local(name, ad)

    def run_actor(self, name, *args, **keys):
        return self._run_actor_mode(name, 'sync', args, keys)
        
    def run_actor_mode(self, name, mode, *args, **keys):
        return self._run_actor_mode(name, mode, args, keys)
    
    def _run_actor_mode(self, name, mode, args, keys):
        actor = self.get_value(name)
        if not actor: raise Exception(f'actor({name}) not found')
        mode_list = mode.split('_')
        at = None
        args = list(args)
        if 'sync' in mode_list: # synchronous call
            at = ActorTransaction(
                actor,
                TransactionType.SYNC,
                args,
                keys
            )
            mode_list.remove('sync')
        elif 'async' in mode_list:
            at = ActorTransaction(
                actor,
                TransactionType.ASYNC,
                args[1:],
                keys,
                args[0]
            )
            mode_list.remove('async')
        elif 'iterator' in mode_list:
            at = ActorTransaction(
                actor,
                TransactionType.ITERATOR,
                args,
                keys
            )            
            mode_list.remove('iterator')
        if not at:
            raise Exception('actor run mode error')
        for m in mode_list:
            if m == 'timed':
                at.is_timed = True
                at.time = args.pop(0)
        return do_run_actor(at)

# class to wrap a subscription to behave as an actor
class SubscriptionClient:
    def __init__(self, msg_type, topic, qos) -> None:
        actor_node.create_subscription(msg_type, topic, self.callback, qos)
        self.callbacks = []
        self.topic = topic
        
    def request(self, callback):
        self.callbacks.append(callback)
    
    def callback(self, res):
        for c in self.callbacks:
            c(res)
    
    def close(self, tran):
        if tran.callback in self.callbacks:
            self.callbacks.remove(tran.callback)
        
class SubSystem(ActorBase):
    def __init__(self, name, parent) -> None:
        self.name = name
        self.subsystem = self
        self.parent = parent        
        self.context = {}
        self.child = {}
        self.subnetwork = []
        super().__init__()
    
    def register_action(self, name, msg_type, topic):
        ac = ActionClient(actor_node, msg_type, topic)
        def stub(callback, goal):
            def res_stub(future):
                callback(future.result().result)
            def response_stub(future):
                r_future = future.result().get_result_async()
                r_future.add_done_callback(res_stub)
            future = ac.send_goal_async(goal)
            future.add_done_callback(response_stub)
        ad = ActorDefinition(stub, name, is_async=True)
        self.set_value_local(name, ad)
        
    def register_subscriber(self, name, msg_type, topic, qos):
        sc = SubscriptionClient(msg_type, topic, qos)
        ad = ActorDefinition(sc.request, name, is_async=True, close=sc.close)
        self.set_value_local(name, ad)   
    
    def register_publisher(self, name, msg_type, topic, qos=10):
        node = self.get_value('node')
        pub = node.create_publisher(msg_type, topic, qos)
        self.register_actor(name, lambda msg: pub.publish(msg), 'sync')          
    
    def add_subsystem(self, name, cls):
        self.child[name] = cls(name, self)
    
    def add_network(self, cls):
        self.subnetwork.append(cls(self))

root_subsystem = SubSystem('root', None)

class SubNet(ActorBase):
    def __init__(self, subsystem):
        self.subsystem = subsystem
        super().__init__()

#############################################################
#   Runtime
#############################################################
active_tran = []

def dump_tran():
    if not verbose: return
#    names = []
#    for t in active_tran:
#        names.append(t.actor.name)
#    print(f'active actors: {names}')
    print(f'num_task: {num_task}')

def term_tran(tran):
    if tran in active_tran:
        active_tran.remove(tran)
    if tran.close: tran.close(tran)

# asynchronous call to a synchronous routine
# intended to be executed as a coroutine of the executor
def sync_async_stub(tran):
    global num_task
    ret = tran.actor.impl(*tran.args)
    tran.callback(ret)
    term_tran(tran)
    num_task -= 1

def async_callback_stub(tran):
    def callback(*args):
        ret = tran.callback(*args)
        if not ret: term_tran(tran)
    return callback

class AsyncActorIterator:
    def __init__(self, tran):
        global it_tran
        it_tran = tran
        self.rets = []
        self.sem = Semaphore(0)
        tran.actor.impl(self.callback, *tran.args, **tran.keys)
        self.tran = tran
        self.open = True
        tran.close = self.close
        tran.abort = self.abort
    
    def callback(self, future):
        self.rets.append(future)
        self.sem.release()
        return True
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
        
    def __iter__(self):
        return self
    
    def __next__(self):
        self.sem.acquire()
        if self.open:
            return self.rets.pop(0)
        else:
            raise StopIteration()
    
    def abort(self, tran):
        self.open = False
        self.sem.release()
    
    def close(self):
        self._close()
        active_tran.remove(self.tran)
        
    def _close(self, *args):
        self.open = False
        self.sem.release()

def do_run_actor(tran):
    global num_task
    tran.start = time.time()
    active_tran.append(tran)
    if tran.tran_type == TransactionType.SYNC:
        if not tran.actor.is_async:
            if not tran.is_extern:
                ret = tran.actor.impl(*tran.args, **tran.keys)
                term_tran(tran)
                return ret
            else:
                event = Event()
                ret = []
                def stub(): 
                    global num_task                   
                    impl_ret = tran.actor.impl(*tran.args, **tran.keys)
                    ret.append(impl_ret)
                    event.set()
                    num_task -= 1
                if num_task >= max_task: raise Exception('insufficient threads')
                actor_executor.create_task(stub)
                num_task += 1
                event.wait()
                term_tran(tran)
                return ret[0]
        else:
            event = Event()
            ret = None
            def stub(*sret):
                nonlocal ret
                ret = sret
                event.set()
            tran.close = tran.actor.close
            tran.actor.impl(stub, *tran.args, **tran.keys)
            event.wait()
            term_tran(tran)
            if len(ret) == 0: return None
            if len(ret) == 1: return ret[0]
            return ret
    elif tran.tran_type == TransactionType.ASYNC:
        if tran.actor.is_async:
            tran.actor.impl(async_callback_stub(tran), *tran.args, **tran.keys)
        else:
            if num_task >= max_task: raise Exception('insufficient threads')
            task = actor_executor.create_task(sync_async_stub, tran)
            tran.task = task
            num_task += 1
    elif tran.tran_type == TransactionType.ITERATOR:
        if tran.actor.is_async:
            ret = AsyncActorIterator(tran)
            tran.close = ret._close
            return ret
    else:
        raise Exception('actor transaction type error')

#############################################################
# bridge to the behavior tree mechanism
#############################################################
def do_register_bt(name):
    return bt_engine.register_bt(name)

class BTEngine:
    def __init__(self):
        self.bt_start = Event()
        self.bt_done = Event()
        
    def register_bt(self, name):
        def stub():
            self.bt_name = name
            self.bt_start.set()
            self.bt_done.wait()
            self.bt_done.clear()
        return ActorDefinition(stub, name)
    
    def run_bt(self):
        id = 0
        while True:
            self.bt_start.wait()
            self.bt_start.clear()
            try:
                node = Node(f'bt_node_{id}')
                id += 1
                lib_main.run_internal(self.bt_name, node)
            except Exception as e:
                print(e)
            self.bt_done.set()     

#############################################################
# bridge to the action server mechanism
#############################################################
class CMActionServer:
    def __init__(self, node):
        self.node = node
        self._action_server = ActionServer(
            node,
            ComplexMotion,
            'complex_motion',
            self.execute_callback)

    def execute_callback(self, goal_handle):
        goal_handle.succeed()
        res = {}
        ex_arg = yaml.safe_load(goal_handle.request.request)
        actor_name = ex_arg['func']
        try:
            arg = ex_arg['arg']
            res['return'] = run_actor(actor_name, *arg)
            res['result'] = True
        except Exception as e:
            print(f'server function error {e}')
            res['result'] = False
        result = ComplexMotion.Result()
        result.result = yaml.dump(res, allow_unicode=True)
        return result

