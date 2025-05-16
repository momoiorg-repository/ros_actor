from enum import Enum
from dataclasses import dataclass
from threading import Thread, Event, Semaphore, Lock
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

from actor_interfaces.action import ComplexMotion

#############################################################
#   API
#############################################################
# 'actor' decorator   
def actor(*a_arg):
    '''
        actor decorator implementation
        handling two types of notations
        @actor
        def func
        
        or 
        
        @actor(name, params...)
        def func
        
        actual registration is done at the constructor of ActorSpace
    '''
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
    '''
        register behavior tree name as an actor
        name: name of the behavior tree
    '''
    actor = do_register_bt(name)
    root_subsystem.set_value_local(name, actor)

def register_subsystem(name, ss):
    '''
        define SubSystem
        name: name of the SubSystem
        ss: parent SubSystem
    '''
    root_subsystem.add_subsystem(name, ss)

def get_value(name):
    '''
        get value from SubSystem context
        name has hierarchical scope 
        name: name of the value
        return: value
    '''
    return root_subsystem.get_value(name)

def set_value(name, value):
    '''
        register a value to the top scope
        name: name of the value
        value: value to be registered
    '''
    root_subsystem.set_value(name, value)

def check_actor(name):
    '''
        check existence of named actor
        name: name of the actor to be checked
        return: bool
    '''
    actor = root_subsystem.get_value(name)
    return (actor and isinstance(actor, ActorBase)) 

def run_actor(name, *l_args, **k_args):
    '''
        run an actor with synchronous mode from external context
        name: name of the actor to be executed
    '''
    actor = get_value(name)
    if not (actor and isinstance(actor, ActorBase)):
        raise Exception(f'run_actor({name}) type error')
    return actor.run(TransactionType.SYNC, l_args, k_args, is_extern=True)

def run_actor_async(name, callback, *l_args, **k_args):
    '''
        run an actor with asynchronous mode from external context
        name: name of the actor to be executed
    '''
    actor = get_value(name)
    if not (actor and isinstance(actor, ActorBase)): raise Exception('run_actor type error')
    return actor.run(TransactionType.ASYNC, l_args, k_args, callback=callback, is_extern=True)

def init_spin(node, max_task_param=15):
    '''
        initialize executor spin.  system gets running
        intended to be call backed from initializer of application program.
        some application library should be initialized before spin starts.
    '''
    global actor_executor, max_task
    max_task = max_task_param
    actor_executor = rclpy.executors.MultiThreadedExecutor(max_task+2)
    actor_executor.add_node(node)
    executor_thread = Thread(target=actor_executor.spin, daemon=True, args=())
    executor_thread.start()
    node.create_rate(1.0).sleep()

def init_server(app_init, verbose_param=False):
    '''
        system initialization and no return
        app_init: call back routine for application program
    '''
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
    '''
        system shutdown
    '''
    rclpy.shutdown()

def get_actor_tran():
    '''
        returns list of active actor transactions
    '''
    return active_tran

verbose = 0
def set_verbose(val):
    '''
        set level of periodical debug print
        0: no display
        1: number of active actors
        2: detail of active actors
    '''
    global verbose
    verbose = val

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
class ActorSpace:
    '''
        base class for SubSystem and SubNet
        - registering actors (constructor)
        - name space management
            only SubSystem has name table.
            SubNet registers name to name space of its parent SubSystem
        - run actor
    '''
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
        if 'sync' in mode_list:
            ad = SyncActor(name, value)
        elif 'async' in mode_list:
            ad = AsyncActor(name, value)
        elif 'multi' in mode_list:
            ad = MultiActor(name, value)
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
        tran = None
        args = list(args)
        if 'sync' in mode_list: # synchronous call
            tran = TransactionType.SYNC
            mode_list.remove('sync')
        elif 'async' in mode_list:
            tran = TransactionType.ASYNC
            mode_list.remove('async')
            callback = args.pop(0)
        elif 'multi' in mode_list:
            tran = TransactionType.MULTI_ASYNC
            mode_list.remove('multi')
            callback = args.pop(0)
        elif 'iterator' in mode_list:
            tran = TransactionType.ITERATOR
            mode_list.remove('iterator')
        if not tran:
            raise Exception('actor run mode error')
        timed = None
        for m in mode_list:
            if m == 'timed':
                timed = args.pop(0)
        if tran == TransactionType.SYNC:
            return actor.run(tran, args, keys, timed=timed)
        elif tran in (TransactionType.ASYNC, TransactionType.MULTI_ASYNC):
            return actor.run(tran, args, keys, callback=callback, timed=timed)
        elif tran == TransactionType.ITERATOR:
            return actor.run(tran, args, keys, timed=timed)

class SubSystem(ActorSpace):
    '''
        implements some method for registering special context
            ROS action, subscriber, publisher
            child SubSystem and SubNet
    '''
    def __init__(self, name, parent) -> None:
        self.name = name
        self.subsystem = self
        self.parent = parent        
        self.context = {}
        self.child = {}
        self.subnetwork = []
        super().__init__()
    
    def register_action(self, name, msg_type, topic):
        aa = ActionActor(self, name, msg_type, topic)
        self.set_value_local(name, aa)
        
    def register_subscriber(self, name, msg_type, topic, qos):
        sa = SubscriberActor(self, name, msg_type, topic, qos)
        self.set_value_local(name, sa)   
    
    def register_publisher(self, name, msg_type, topic, qos=10):
        pa = PublisherActor(self, name, msg_type, topic, qos)
        self.set_value_local(name, pa)   
    
    def add_subsystem(self, name, cls):
        self.child[name] = cls(name, self)
    
    def add_network(self, cls):
        self.subnetwork.append(cls(self))

root_subsystem = SubSystem('root', None)

class SubNet(ActorSpace):
    '''
        implementation of SubNet
    '''
    def __init__(self, subsystem):
        self.subsystem = subsystem
        super().__init__()

#############################################################
#   Definition of Actors
#############################################################
# id for 4 types of actor invocation
class TransactionType(Enum):
    SYNC = 1
    ASYNC = 2
    ITERATOR = 3
    MULTI_ASYNC = 4

class ActorBase:
    '''
        base class for various actor classes
        keeps definition of each actor and implements its invocation
    '''
    def __init__(self, name, impl):
        self.name = name
        self.impl = impl
    
    def run(self, tran_type, args, keys, callback=None, timed=None, is_extern=False):
        if timed:
            tran = ActorTransaction(self, tran_type, args, keys, callback,
                                  is_timed=True, time=timed, is_extern=is_extern)
        else:
            tran = ActorTransaction(self, tran_type, args, keys, callback,
                                    is_extern=is_extern)

        def abort_stub():
            self.abort(tran)
        def close_stub():
            self.close(tran)

        tran.abort = abort_stub
        tran.close = close_stub
        tran.start = time.time()
        with tran_lock:
            active_tran.append(tran)
        if tran_type == TransactionType.SYNC:
            return self.sync_run(tran)
        elif tran_type == TransactionType.ASYNC:
            return self.async_run(tran)
        elif tran_type == TransactionType.ITERATOR:
            return self.iterator_run(tran)
        else:
            return self.multi_run(tran)

    def sync_run(self, tran): raise Exception('illegal transaction type')
    
    def async_run(self, tran): raise Exception('illegal transaction type')
    
    def iterator_run(self, tran): raise Exception('illegal transaction type')
    
    def multi_run(self, tran): raise Exception('illegal transaction type')
    
    def abort(self, tran):
        with tran_lock:
            if tran in active_tran:
                active_tran.remove(tran)
        tran.is_active = False
    
    def close(self, tran):
        with tran_lock:
            if tran in active_tran:
                active_tran.remove(tran)
        tran.is_active = False
                
# regular sync actor
class SyncActor(ActorBase):    
    '''
        definition for regular synchronous actors
    '''
    def sync_run(self, tran):
        if not (tran.is_extern or tran.is_timed):
            ret = self.impl(*tran.args, **tran.keys)
            self.close(tran)
            return ret
        else:
            global num_task
            sem = Semaphore(0)
            ret = None
            def stub(): 
                global num_task
                nonlocal ret                   
                ret = self.impl(*tran.args, **tran.keys)
                sem.release()
                num_task -= 1
            if num_task >= max_task: raise Exception('insufficient threads')
            actor_executor.create_task(stub)
            num_task += 1
            sem.acquire()
            self.close(tran)
            return ret
   
    # asynchronous call to a synchronous routine
    # intended to be executed as a coroutine of the executor
    def sync_async_stub(self):
        def stub(tran):
            global num_task
            ret = tran.actor.impl(*tran.args)
            if tran.is_active:
                tran.callback(ret)
                self.close(tran)
                num_task -= 1
        return stub
        
    def abort_async(self, tran):
        global num_task
        tran.callback(None)
        self.close(tran)
        num_task -= 1
    
    def async_run(self, tran):
        global num_task, actor_executor
        tran.abort = self.abort_async
        if num_task >= max_task: raise Exception('insufficient threads')
        task = actor_executor.create_task(self.sync_async_stub(), tran)
        tran.task = task
        num_task += 1
        return tran

# regular async actor
class AsyncActor(ActorBase):
    '''
        definition for regular asynchronous actors which requires callback function at completion
    '''
    def sync_run(self, tran):
        global num_task
        sem = Semaphore(0)
        ret = None
        def stub(local_ret): 
            global num_task
            nonlocal ret
            ret = local_ret
            sem.release()
            num_task -= 1
        if num_task >= max_task: raise Exception('insufficient threads')
        actor_executor.create_task(lambda : self.impl(stub, *tran.args, **tran.keys))
        num_task += 1
        sem.acquire()
        self.close(tran)
        return ret
    
    def async_run(self, tran):
        def stub(tran):
            def callback(*args, **keys):
                tran.callback(*args, **keys)
                self.close(tran)
            return callback
        desc = self.impl(stub(tran), *tran.args, **tran.keys)
        if desc:
            for k, v in desc:
                if k == 'close':
                    org_close = tran.close
                    def close_stub(tran):
                        v(tran)
                        org_close(tran)
                    tran.close = close_stub
        return tran

class MultiActorIterator:
    '''
        implementation of a Python Iterator using multi mode actor
    '''
    def __init__(self, tran):
        global it_tran
        it_tran = tran
        self.rets = []
        self.sem = Semaphore(0)
        tran.actor.impl(self.callback, *tran.args, **tran.keys)
        self.tran = tran
        self.open = True
        tran.close = self.close
        tran.abort = self.close
    
    def callback(self, future):
        if self.open:
            self.rets.append(future)
            self.sem.release()
            return True
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self._close()
        
    def __iter__(self):
        return self
    
    def __next__(self):
        self.sem.acquire()
        if self.open:
            return self.rets.pop(0)
        else:
            raise StopIteration()
    
    def close(self, tran):
        self._close()
        
    def _close(self):
        self.open = False
        self.rets = []
        self.sem.release()
        tran = self.tran
        tran.actor.close(tran)

class MultiActor(AsyncActor):
    '''
        definition for regular asynchronous actors
        it is similar to asynchronous actor but generates multiple callback calls until close or abort is called
    '''
    def multi_run(self, tran):
        def stub(*args, **keys):
            if not tran.is_active: return
            tran.callback(*args, **keys)
        desc = self.impl(stub, *tran.args, **tran.keys)
        if desc:
            for k, v in desc:
                if k == 'close':
                    org_close = tran.close
                    def close_stub(tran):
                        v(tran)
                        org_close(tran)
                    tran.close = close_stub
        return tran
    
    def iterator_run(self, tran):
        return MultiActorIterator(tran)

# actor wrapping for subscriber 
class SubscriberActor(MultiActor):
    '''
        emulates a multi mode actor using ROS subscriber
    '''
    def __init__(self, env, name, msg_type, topic, qos):
        self.env = env
        actor_node = env.get_value('node')
        actor_node.create_subscription(msg_type, topic, self.callback, qos)
        self.callbacks = []
        self.callback_lock = Lock()
        self.topic = topic
        super().__init__(name, self.request)
        
    def request(self, callback):
        with self.callback_lock:
            self.callbacks.append(callback)
    
    def callback(self, res):
        for c in self.callbacks:
            c(res)
    
    def close(self, tran):
        with self.callback_lock:
            if tran.callback in self.callbacks:
                self.callbacks.remove(tran.callback)
        super().close(tran)
        
# actor wrapping for publisher
class PublisherActor(SyncActor):
    '''
        emulates synchronous mode actor using ROS publisher
    '''
    def __init__(self, env, name, msg_type, topic, qos=10):
        self.env = env
        node = env.get_value('node')
        pub = node.create_publisher(msg_type, topic, qos)
        super().__init__(name, lambda msg: pub.publish(msg))

# actor wrapping for action client
class ActionActor(AsyncActor):
    '''
        emulates asynchronous mode actor using ROS action client
    '''
    def __init__(self, env, name, msg_type, topic):
        self.env = env
        node = env.get_value('node')
        ac = ActionClient(node, msg_type, topic)
        
        def stub(callback, goal):
            def result_stub(future):
                callback(future.result().result)
            def response_stub(future):
                r_future = future.result().get_result_async()
                r_future.add_done_callback(result_stub)
            future = ac.send_goal_async(goal)
            future.add_done_callback(response_stub)
            
        super().__init__(name, stub)

#############################################################
#   Runtime
#############################################################
# represent actor transaction: core data structure
@dataclass
class ActorTransaction:
    '''
        represents execution of actor(= transaction)
    '''
    actor: ActorBase
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
    is_active: bool = True
    
# counter for statistics
num_task = 0
max_task = 0

# 'ready queue' for actors
active_tran = []
tran_lock = Lock()

def dump_tran():
    if verbose <= 0: return
    print(f'num_task: {num_task}')
    if verbose <= 1: return
    names = []
    for t in active_tran:
        names.append(t.actor.name)
    print(f'active actors: {names}')

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

#############################################################
# bridge to the behavior tree mechanism
#############################################################
def do_register_bt(name):
    return bt_engine.register_bt(name)

class BTEngine:
    '''
        running behavior tree by using pytwb library
        behave as a synchronous mode actor
    '''
    def __init__(self):
        self.bt_start = Event()
        self.bt_done = Event()
        
    def register_bt(self, name):
        def stub():
            self.bt_name = name
            self.bt_start.set()
            self.bt_done.wait()
            self.bt_done.clear()
        return SyncActor(name, stub)
    
    def run_bt(self):
        id = 0
        while True:
            self.bt_start.wait()
            self.bt_start.clear()
            try:
                node = Node(f'bt_node_{id}')
                id += 1
                lib_main.run_internal(self.bt_name, node)
                node.destroy_node()
            except Exception as e:
                print(e)
            self.bt_done.set()     

#############################################################
# bridge to the action server mechanism
#############################################################
class CMActionServer:
    '''
        ROS action server
        every actor can be invoked from other nodes through ROS action mechanism
    '''
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
