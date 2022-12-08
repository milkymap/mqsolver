import zmq 
import threading
import multiprocessing as mp 

from threading import Thread, Event
from multiprocessing.context import Process

from typing import List, Union, Type, Dict

from log import logger 

from engine.source import ZMQSource
from engine.switch import ZMQSwitch
from engine.target import ZMQTarget

from schema.config import WorkerConfig, SwitchConfig, RUNNER_MODE
from schema.zmqconfig import FSTConfig, SNDConfig, CCR_SND_Config, PRL_SND_Config

from solver_starting_protocol import CCRSolverStarter, PRLSolverStarter, SolverStartingProtocol


class ZMQRunner:
    def __init__(self, runner_id:int, worker_config:WorkerConfig, fst_address_settings:FSTConfig, runner_mode:RUNNER_MODE):
        self.runner_id = runner_id
        self.runner_mode = runner_mode

        self.max_nb_running_tasks = worker_config.max_nb_running_tasks
        self.list_of_switch_configs = worker_config.list_of_switch_configs

        self.nb_switchs = len(self.list_of_switch_configs)
        self.shutdown_signal = threading.Event()

        self.threads_started:bool = False 
        self.threads_initialization:bool = False 
        self.fst_address_settings = fst_address_settings

        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)

        self.map_runner_mode2snd_config_builder:Dict[RUNNER_MODE, Type[SNDConfig]] = {
            RUNNER_MODE.CONCURRENT:CCR_SND_Config,
            RUNNER_MODE.PARALLEL:PRL_SND_Config
        }

        self.map_runner_mode2solver_starting_protocol:Dict[RUNNER_MODE, SolverStartingProtocol] = {
            RUNNER_MODE.CONCURRENT: CCRSolverStarter(),  # start solver in thread mode 
            RUNNER_MODE.PARALLEL: PRLSolverStarter()  # start solver in process mode 
        }

        self.map_runner_mode2solver_builder:Dict[RUNNER_MODE, Union[Type[Thread], Type[Process]]] = {
            RUNNER_MODE.CONCURRENT:Thread, 
            RUNNER_MODE.PARALLEL:Process
        }

    def start_all_services(self) -> int:
        if not self.threads_initialization:
            return 1 
        
        try:
            
            self.source_thread.start()
            for switch_thread in self.switch_thread_array:
                switch_thread.start()
            
            self.target_thread.start()
            
            for solver_worker in self.solver_worker_array:
                solver_worker.start()
            
            self.threads_started = True 

            for solver_worker in self.solver_worker_array:
                solver_worker.join()
            
            
            for switch_thread in self.switch_thread_array:
                switch_thread.join()
            
            self.source_thread.join()

            self.target_thread.join()
        except KeyboardInterrupt:
            logger.debug(f'runner {self.runner_id:03d} has catched the SIGINT signal')
            if not self.shutdown_signal.is_set():
                self.shutdown_signal.set()
                logger.debug(f'runner {self.runner_id:03d} has set the shutdown signal to notify workers')
        except Exception as e:
            logger.error(e)
            return 1       
        return 0 
 
        
    def start_source(self):
        source_ = ZMQSource(
            ctx=self.ctx,
            nb_switchs=self.nb_switchs,
            shutdown_signal=self.shutdown_signal,
            max_nb_running_tasks=self.max_nb_running_tasks,
            fst_address_settings=self.fst_address_settings
        )

        with source_ as src:
            src.start_loop()

    def start_switch(self, switch_id:int, switch_config:SwitchConfig, snd_address_settings:SNDConfig):
        switch_ = ZMQSwitch(
            ctx=self.ctx,
            switch_id=switch_id,
            switch_config=switch_config,
            shutdown_signal=self.shutdown_signal,
            fst_address_settings=self.fst_address_settings,
            snd_address_settings=snd_address_settings
        )

        with switch_ as swt:
            swt.start_loop()

    def start_target(self):
        target_ = ZMQTarget(
            ctx=self.ctx,
            shutdown_signal=self.shutdown_signal,
            fst_address_settings=self.fst_address_settings
        )

        with target_ as trg:
            trg.start_loop()

    def build_solver_worker(self, starter:SolverStartingProtocol, builder:Union[Type[Thread], Type[Process]], **kwargs) -> Union[Thread, Process]:
        solver_worker = builder(
            target=starter.start_solver,
            kwargs=kwargs
        )

        return solver_worker
                    

    def __enter__(self):
        try:
            self.source_thread = Thread(target=self.start_source)
            self.target_thread = Thread(target=self.start_target)

            self.switch_thread_array:List[Thread] = []
            self.solver_worker_array:List[Union[Thread, Process]] = []  # can be List[Thread] of List[Process]

            for switch_id in range(self.nb_switchs):
                snd_address_settings = self.map_runner_mode2snd_config_builder[self.runner_mode](switch_id=switch_id)
                switch_config = self.list_of_switch_configs[switch_id]
                nb_solvers = switch_config.nb_solvers
                service_name = switch_config.service_name
                solver_factory = switch_config.solver_factory
                
                
                switch_thread = Thread(
                    target=self.start_switch, 
                    kwargs={
                        'switch_id': switch_id,
                        'switch_config':  switch_config, 
                        'snd_address_settings': snd_address_settings 
                    }
                )
                self.switch_thread_array.append(switch_thread)
                
                for solver_id in range(nb_solvers):
                    opts = {
                        'ctx': self.ctx, 
                        'shutdown_signal': self.shutdown_signal, 
                        'solver_id': solver_id,
                        'switch_id': switch_id,
                        'service_name': service_name,
                        'solver_factory': solver_factory, 
                        'fst_address_settings': self.fst_address_settings,
                        'snd_address_settings': snd_address_settings 
                    }
                    solver_worker = self.build_solver_worker(
                        starter=self.map_runner_mode2solver_starting_protocol[self.runner_mode],
                        builder=self.map_runner_mode2solver_builder[self.runner_mode], 
                        **opts 
                    )
                        
                    self.solver_worker_array.append(solver_worker) 
            
            self.threads_initialization = True
        except Exception as e:
            logger.error(e)
        return self 

    def __exit__(self, exc_type, exc_value, traceback):
        if self.threads_started:
            self.target_thread.join()
            logger.debug(f'runner {self.runner_id:03d} has released all threads')

        logger.debug(f'runner {self.runner_id:03d} will end the context')
        self.ctx.term()
        logger.debug(f'runner {self.runner_id:03d} has remove its zeromq context')
        return True 