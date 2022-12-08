import zmq 
import threading 
import multiprocessing as mp 

from math import ceil 
from typing import List, Optional
from time import sleep, time, perf_counter
          
from log import logger 

from runner import ZMQRunner
from schema.task import Tasks, TaskCacheResponse
from schema.config import WorkerConfig, RUNNER_MODE
from schema.zmqconfig import FSTConfig, CCR_FST_Config, PRL_FST_Config

class ZMQServer:
    def __init__(self, worker_config:WorkerConfig, nb_runners:int, address_settings:FSTConfig, runner_mode:RUNNER_MODE):
        assert nb_runners > 0 

        self.nb_runners = nb_runners
        self.worker_config = worker_config

        self.runners_started:bool = False 
        self.runners_initialization:bool = False 

        self.socket_initialization:bool = False 
        self.address_settings = address_settings
        self.target2server_pull_address = address_settings.TARGET2SERVER_PULL
        self.server2source_push_address = address_settings.SERVER2SOURCE_PUSH

        self.runner_mode = runner_mode
        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)

    def start_runner(self, runner_id:int) -> None:
        try:
            runner_ = ZMQRunner(
                runner_id=runner_id,
                worker_config=self.worker_config,
                fst_address_settings=self.address_settings, 
                runner_mode=self.runner_mode
            ) 
            with runner_ as rnr:
                rnr.start_all_services()
        except Exception as e:
            logger.error(e)

    def start_all_runners(self) -> int:
        if not self.runners_initialization:
            logger.warning('server was not able to initialize the runners')
            return 1 
        
        runner_process:mp.Process
        try:
            for runner_process in self.runner_processes:
                runner_process.start()       
            self.runners_started = True 
        except Exception as e:
            logger.error(e)
        return 0 
    
    def submit_tasks(self, tasks:Tasks) -> Optional[TaskCacheResponse]:
        if not self.runners_started:
            logger.warning('runners are not availables yet')
            return None  

        nb_tasks = len(tasks)
        if nb_tasks > self.nb_runners:
            partition_size = ceil(nb_tasks / self.nb_runners)
        else:
            partition_size = nb_tasks

        nb_effective_runners = 0
        for cursor in range(0, nb_tasks, partition_size):
            # send tasks_partition to remote runners in a round-robin strategy 
            tasks_partition = tasks[cursor:cursor+partition_size]
            self.server2source_push_socket.send_pyobj(tasks_partition)
            cursor = cursor + partition_size 
            logger.debug(f'server has sent {len(tasks_partition):03d} tasks to the runners')
            nb_effective_runners += 1

        accumulator:TaskCacheResponse = {}
        keep_loop = True 
        nb_responses = 0
        start_timer = perf_counter()
        while keep_loop:
            try:
                if nb_responses == nb_effective_runners:
                    keep_loop = False 
                
                target2server_status = self.target2server_pull_socket.poll(timeout=100)
                if target2server_status == zmq.POLLIN:
                    target_response:TaskCacheResponse = self.target2server_pull_socket.recv_pyobj()
                    accumulator.update(target_response)
                    nb_responses = nb_responses + 1 
            except KeyboardInterrupt:
                keep_loop = False 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop ...! 
        end_timer = perf_counter()
        duration = end_timer - start_timer
        logger.debug(f'this request took {duration:05.3f} seconds to complete {len(accumulator)} tasks')
        return accumulator
            
    def __enter__(self):
        try:
            self.server2source_push_socket:zmq.Socket = self.ctx.socket(zmq.PUSH)
            self.server2source_push_socket.bind(self.server2source_push_address)

            self.target2server_pull_socket:zmq.Socket = self.ctx.socket(zmq.PULL)
            self.target2server_pull_socket.bind(self.target2server_pull_address)

            self.socket_initialization = True 

            self.runner_processes:List[mp.Process] = []
            for runner_id in range(self.nb_runners):
                runner_process = mp.Process(
                    target=self.start_runner,
                    kwargs={
                        'runner_id': runner_id, 
                    }
                )
                self.runner_processes.append(runner_process)
                
            self.runners_initialization = True 
        except Exception as e:
            logger.error(e)
        return self 
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.runners_started:
            logger.debug('HIT CTL+C button to force runners to exit')
            runner_process:mp.Process
            try:
                for runner_process in self.runner_processes:
                    runner_process.join()
            except KeyboardInterrupt:
                logger.debug('server is waiting for runner to quit')
                for runner_process in self.runner_processes:
                    runner_process.join() 

        if self.socket_initialization:
            self.target2server_pull_socket.close()
            self.server2source_push_socket.close()
            logger.debug('server has closed its socket')

        self.ctx.term()
        logger.debug('server has exited its event loop')
        return True 

class CCRServer(ZMQServer):
    def __init__(self, worker_config: WorkerConfig, nb_runners: int):
        super().__init__(
            worker_config=worker_config, 
            nb_runners=nb_runners,
            address_settings=CCR_FST_Config(),
            runner_mode=RUNNER_MODE.CONCURRENT
        )

class PRLServer(ZMQServer):
    def __init__(self, worker_config: WorkerConfig):
        super().__init__(
            worker_config=worker_config, 
            nb_runners=1, 
            address_settings=PRL_FST_Config(),
            runner_mode=RUNNER_MODE.PARALLEL
        )