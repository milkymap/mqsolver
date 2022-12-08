import zmq 
import pickle 
import threading

from typing import List, Dict, Any, Optional, Callable
from threading import Event
from time import time 

from log import logger 

from base_solver import ABCSolver
from time import perf_counter

from schema.task import SpecializedTask 
from schema.worker import WorkerStatus, SolverJoinResponse, SolverDataResponse
from schema.signal import SignalValue
from schema.zmqconfig import FSTConfig, SNDConfig, CCR_FST_Config, CCR_SND_Config, PRL_FST_Config, PRL_SND_Config

class ZMQSolver:
    def __init__(
        self, 
        solver_id:int,
        switch_id:int,
        service_name:str,
        solver_factory:Callable[..., ABCSolver],
        fst_address_settings:FSTConfig,
        snd_address_settings:SNDConfig
        ):

        self.ctx:zmq.Context

        self.solver_id = solver_id 
        self.switch_id = switch_id 
        self.service_name = service_name
            
        self.solver_factory = solver_factory 
        self.solver2target_pull_address = fst_address_settings.SOLVER2TARGET_PULL
            
        self.switch2solver_push_addresss = snd_address_settings.SWITCH2SOLVER_PUSH
        self.switch_solver_router_address = snd_address_settings.SWITCH_SOLVER_ROUTER
        self.switch2solver_publisher_address = snd_address_settings.SWITCH2SOLVER_PUBLISHER
    
        self.solver_strategy:Optional[ABCSolver]=None 
        self.sockets_initialization:bool = False 
    
    def monitor_loop(self) -> bool:
        return False 
        
    def notify_workers(self) -> None:
        pass 

    def remove_zmqcontext(self) -> None:
        pass 
    
    def check_signal_from_switch(self, signal_value:SignalValue, timeout:int=5000) -> bool:
        subscriber_socket_status = self.switch2solver_subscriber_socket.poll(timeout=timeout)
        if subscriber_socket_status == zmq.POLLIN: 
            incoming_topic, _ = self.switch2solver_subscriber_socket.recv_multipart()
            if incoming_topic == signal_value:
                return True   
        return False  

    def start_loop(self) -> int:
        if not self.sockets_initialization:
            logger.debug(f'solver {self.solver_id:03d} was not able to create its sockets to join the switch {self.switch_id:03d}')
            return 1 
        
        logger.debug(f'solver {self.solver_id:03d} has created its sockets to join the switch {self.switch_id:03d}')      
            
        self.switch_solver_dealer_socket.send_string('', flags=zmq.SNDMORE)
        self.switch_solver_dealer_socket.send_pyobj(
            SolverDataResponse(
                response_type=WorkerStatus.JOIN, 
                response_content=SolverJoinResponse(
                    solver_id=self.solver_id
                )
            )
        )
        
        readyness_signal = self.check_signal_from_switch(SignalValue.IS_READY, 5000)
        if not readyness_signal:
            logger.warning(f'solver {self.switch_id:03d} has waited too long to get (readyness signal) from the switch {self.switch_id:03d}')
            return 1 

        logger.debug(f'solver {self.solver_id:03d} has gotten the (readyness signal) the switch {self.switch_id:03d}')
        
        nb_hits = 0
        keep_loop = True 
        while keep_loop:
            if self.monitor_loop():
                break 
            
            try:
                
                push_socket_status = self.switch2solver_pull_socket.poll(timeout=100)
                if push_socket_status == zmq.POLLIN: 
                    if self.solver_strategy is not None:
                        switch_encoded_message = self.switch2solver_pull_socket.recv()
                        logger.debug(f'solver {self.solver_id:03d} has got a new task from {self.switch_id:03d}')
                        switch_plain_message:SpecializedTask = pickle.loads(switch_encoded_message)
                        X = perf_counter()
                    
                        response:SolverDataResponse = self.solver_strategy(switch_plain_message)
                        D = perf_counter() - X 
                        self.solver2target_push_socket.send_pyobj((self.service_name, response))       
                        logger.debug(f'solver {self.solver_id:03d} has consumed the task {switch_plain_message.task_id} in {D:05.3f} seconds')
                        nb_hits += 1
                else:
                    logger.debug(f'solver {self.solver_id:03d} is waiting to get a new task from the switch {self.service_name}')
            except KeyboardInterrupt:
                keep_loop = False 
            except Exception as e:
                logger.error(e)
                keep_loop = False       
        # end while loop 
        logger.debug(f'solver {self.solver_id:03d} has exited its loop and will leave the switch {self.switch_id:03d}')
        logger.debug(f'solver {self.solver_id:03d} has received {nb_hits:03d} tasks from the switch {self.switch_id:03d}')
        
        return 0 

    def __enter__(self):
        try:
            self.solver_strategy = self.solver_factory()
            self.solver_strategy.initialize()
        except Exception as e:
            logger.error(e)
            
        if self.solver_strategy is not None:
            try:    
                self.solver2target_push_socket:zmq.Socket = self.ctx.socket(zmq.PUSH)
                self.solver2target_push_socket.connect(self.solver2target_pull_address)

                self.switch_solver_dealer_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
                self.switch_solver_dealer_socket.connect(self.switch_solver_router_address)

                self.switch2solver_subscriber_socket:zmq.Socket = self.ctx.socket(zmq.SUB)
                self.switch2solver_subscriber_socket.setsockopt(zmq.SUBSCRIBE, SignalValue.IS_ALIVE)
                self.switch2solver_subscriber_socket.setsockopt(zmq.SUBSCRIBE, SignalValue.IS_READY)
                self.switch2solver_subscriber_socket.setsockopt(zmq.SUBSCRIBE, SignalValue.QUITLOOP)
                self.switch2solver_subscriber_socket.connect(self.switch2solver_publisher_address)

                self.switch2solver_pull_socket:zmq.Socket = self.ctx.socket(zmq.PULL)
                self.switch2solver_pull_socket.connect(self.switch2solver_push_addresss)

                self.sockets_initialization = True 
            except Exception as e:
                logger.error(e)
            
        return self 
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.notify_workers()

        if self.solver_strategy is not None:
            if isinstance(self.solver_strategy, ABCSolver):
                self.solver_strategy.destroy_ressources()

        if self.sockets_initialization:
            logger.debug(f'solver {self.solver_id:03d} will end its zeromq context to quit the switch {self.switch_id:03d}')
            
            self.switch2solver_pull_socket.close()
            self.solver2target_push_socket.close()
            self.switch_solver_dealer_socket.close()
            self.switch2solver_subscriber_socket.close()
            logger.debug(f'solver {self.solver_id:03d} has released its sockets and leave the switch {self.switch_id:03d}')
        
        self.remove_zmqcontext()
            
        return True 


class CCRSolver(ZMQSolver):
    def __init__(self, ctx:zmq.Context, shutdown_signal:Event, solver_id: int, switch_id: int, service_name: str, solver_factory: Callable[..., ABCSolver], fst_address_settings: FSTConfig, snd_address_settings:SNDConfig):
        assert isinstance(fst_address_settings, CCR_FST_Config)
        assert isinstance(snd_address_settings, CCR_SND_Config)
        
        super(CCRSolver, self).__init__(solver_id, switch_id, service_name, solver_factory, fst_address_settings, snd_address_settings)
        self.ctx = ctx 
        self.shutdown_signal = shutdown_signal
    
    def monitor_loop(self) -> bool:
        return self.shutdown_signal.is_set()
    
    def notify_workers(self) -> None:
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()
        
    def remove_zmqcontext(self) -> None:
        return super().remove_zmqcontext()


class PRLSolver(ZMQSolver):
    def __init__(self, solver_id: int, switch_id: int, service_name: str, solver_factory: Callable[..., ABCSolver], fst_address_settings: FSTConfig, snd_address_settings:SNDConfig):
        assert isinstance(fst_address_settings, PRL_FST_Config)
        assert isinstance(snd_address_settings, PRL_SND_Config)
        
        super().__init__(solver_id, switch_id, service_name, solver_factory, fst_address_settings, snd_address_settings)
        self.ctx = zmq.Context()
    
    def monitor_loop(self) -> bool:
        return super().monitor_loop()

    def notify_workers(self) -> None:
        return super().notify_workers()

    def remove_zmqcontext(self) -> None:
        self.ctx.term()