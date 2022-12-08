import zmq 
import pickle 
import threading

from typing import List, Dict, Any, Optional, Tuple 
from threading import Event
from time import time, sleep , perf_counter


from log import logger 
from schema.task import SpecializedTask, Topic, Tasks, TaskResponseData
from schema.worker import (
    SwitchJoinResponse, 
    SwitchDataResponse, 
    SourceDataResponse, 
    SourceBusyResponse, 
    SourceDoneResponse,
    SourceFreeResponse,
    TargetDataResponse, 
    TargetFreeResponse
)

from schema.signal import SignalValue
from schema.zmqconfig import FSTConfig

class ZMQSource:
    def __init__(
        self, 
        ctx:zmq.Context,
        nb_switchs:int,
        shutdown_signal:Event,   
        max_nb_running_tasks:int,  
        fst_address_settings:FSTConfig, 
        ):

        self.ctx = ctx 

        self.nb_switchs = nb_switchs
        self.shutdown_signal = shutdown_signal

        self.max_nb_running_tasks = max_nb_running_tasks

        self.server2source_push_address = fst_address_settings.SERVER2SOURCE_PUSH

        self.source_target_pair_address = fst_address_settings.SOURCE_TARGET_PAIR
        self.source_switch_router_address = fst_address_settings.SOURCE_SWITCH_ROUTER
        self.source2switch_publisher_address = fst_address_settings.SOURCE2SWITCH_PUBLISHER
        
        self.sockets_initialization:bool = False 

        self.map_topics2nb_switchs:Dict[Topic, int] = {}
    
    def synchronize_with_switchs(self, max_duration:float=10.0) -> bool:
        start_timer = time()
        counter = 0
        keep_loop = True 
        while keep_loop:
            router_socket_status = self.source_switch_router_socket.poll(100)
            if router_socket_status == zmq.POLLIN: 
                _, _, switch_encoded_message = self.source_switch_router_socket.recv_multipart()
                switch_plain_message:SwitchDataResponse = pickle.loads(switch_encoded_message)
                if isinstance(switch_plain_message.response_content, SwitchJoinResponse):
                    response_content:SwitchJoinResponse = switch_plain_message.response_content
                    for topic in response_content.list_of_topics:
                        if topic not in self.map_topics2nb_switchs:
                            self.map_topics2nb_switchs[topic] = 0
                        self.map_topics2nb_switchs[topic] += 1 
                    counter = counter + 1
              
            duration = time() - start_timer
            if duration > max_duration or counter == self.nb_switchs:
                keep_loop = False 
        # end while loop 

        return self.nb_switchs == counter 


    def start_loop(self) -> int:
        if not self.sockets_initialization:
            logger.debug('source was not able to initialize its sockets')
            return 1 
        
        logger.debug('source has created its sockets')
        
        synchronize_status = self.synchronize_with_switchs()
        if not synchronize_status:
            logger.warning('source was not able to make syncrhonization with switchs')
            return 1 
        
        logger.debug('source has performed the synchronization with the switchs')
        
        # notify switch 
        self.source2switch_publisher_socket.send_multipart([SignalValue.IS_READY, b''])
        start_timer = time()
        tasks:Optional[Tasks] = None
        cursor = 0
        keep_loop = True
        nb_running_tasks = 0 
        X = 0.0
        while keep_loop:
            try:
                if self.shutdown_signal.is_set():
                    break   

                if tasks is not None and cursor >= len(tasks) and nb_running_tasks == 0:
                    self.source_target_pair_socket.send_pyobj(
                        SourceDataResponse(
                            response_content=SourceFreeResponse()
                        )
                    )
                    logger.debug(f'source has sent all {cursor:03d} tasks and ask target to send the result to server')
                    tasks = None 
                    print('duration => ', perf_counter() - X)
        
                    
                if tasks is None:
                    server2source_status = self.server2source_pull_socket.poll(timeout=100)
                    if server2source_status == zmq.POLLIN:
                        # source received a list of tasks from server 
                        # source can not receive two list of tasks at the same time
                        tasks = self.server2source_pull_socket.recv_pyobj()
                        nb_running_tasks = 0 
                        cursor = 0
                        X = perf_counter()

                if tasks is not None and cursor < len(tasks) and nb_running_tasks < self.max_nb_running_tasks:
                    generic_task = tasks[cursor]
                    task_id = generic_task.task_id
                    task_content = generic_task.task_content
                    for topic in generic_task.list_of_topics:  # no duplicate topics 
                        if topic in self.map_topics2nb_switchs and self.map_topics2nb_switchs[topic] > 0:
                            nb_subscribers = self.map_topics2nb_switchs[topic]
                            specialized_task = SpecializedTask(
                                topic=topic, 
                                task_id=task_id, 
                                task_content=task_content
                            )
                            self.source2switch_publisher_socket.send_string(topic, flags=zmq.SNDMORE)
                            self.source2switch_publisher_socket.send_pyobj(obj=specialized_task)
                            nb_running_tasks += nb_subscribers 
             
                            logger.debug(f'task {task_id} was schedulled | nb running task : {nb_running_tasks:05d}')

                        else: 
                            self.source_target_pair_socket.send_pyobj(
                                SourceDataResponse(
                                    response_content=SourceDoneResponse(
                                        response_content=TaskResponseData(
                                            task_id=task_id,
                                            topic=topic,
                                            response_content=None
                                        )
                                    )
                                )
                                
                            )
                            logger.debug(f'task {task_id} has no target subscriber solvers, it will be discarded')
                    # end loop over topics ...! 
                    cursor = cursor + 1 
                else:
                    end_timer = time()
                    duration = end_timer - start_timer
                    if duration > 0.05 and tasks is not None:
                        # source has reached the maximum number of running task or there is no tasks left 
                        self.source_target_pair_socket.send_pyobj(
                            SourceDataResponse(
                                response_content=SourceBusyResponse()
                            )
                        )  # ask target to send us back its current state
                        target_plain_message:TargetDataResponse = self.source_target_pair_socket.recv_pyobj()
                        if isinstance(target_plain_message.response_content, TargetFreeResponse):
                            target_free_response:TargetFreeResponse = target_plain_message.response_content
                            nb_running_tasks -= target_free_response.nb_collected_responses
                        start_timer = end_timer
                    # ...! ask an update from the target for each 1s

            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop
        return 0
        
    def __enter__(self):
        try:

            self.source2switch_publisher_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
            self.source2switch_publisher_socket.bind(self.source2switch_publisher_address)

            self.source_switch_router_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            self.source_switch_router_socket.bind(self.source_switch_router_address)

            self.source_target_pair_socket:zmq.Socket = self.ctx.socket(zmq.PAIR)
            self.source_target_pair_socket.bind(self.source_target_pair_address)

            self.server2source_pull_socket:zmq.Socket = self.ctx.socket(zmq.PULL)
            self.server2source_pull_socket.connect(self.server2source_push_address)

            self.sockets_initialization = True # notify switchs to start
        except Exception as e:
            logger.error(e)

        return self 
    
    def __exit__(self, exc_type, exc_value, traceback):
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()

        if self.sockets_initialization:
            
            self.source_target_pair_socket.close()
            self.source_switch_router_socket.close()
            self.source2switch_publisher_socket.close()
            self.server2source_pull_socket.close()
            
            logger.debug('source has released its sockets')
        
        return True 
