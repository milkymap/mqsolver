import zmq 
import threading

from typing import Tuple

from schema.zmqconfig import FSTConfig
from schema.task import TaskStatus, TaskCacheResponse, TaskResponseData
from schema.worker import (
    SolverDataResponse, SourceDataResponse, SourceBusyResponse,  SourceFreeResponse,
    SourceDoneResponse, SolverDoneResponse, WorkerStatus, TargetDataResponse, TargetFreeResponse
)
from log import logger 

class ZMQTarget:
    def __init__(
        self,
        ctx:zmq.Context, 
        shutdown_signal:threading.Event,
        fst_address_settings:FSTConfig,
        ):

        self.ctx = ctx 
        self.shutdown_signal = shutdown_signal

        self.target2server_pull_address = fst_address_settings.TARGET2SERVER_PULL
        self.solver2target_pull_address = fst_address_settings.SOLVER2TARGET_PULL
        self.source_target_pair_address = fst_address_settings.SOURCE_TARGET_PAIR

        self.sockets_initialization:bool = False 

        # map task_id => map topic => map service_name => data 
        self.cache_responses:TaskCacheResponse = {}
    
    def start_loop(self) -> int:
        if not self.sockets_initialization:
            logger.warning('target was not able to initialze its sockets')
            return 1 
        logger.debug('target has initialized its zeromq sockets')
        
        nb_responses = 0
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                break 
            try:
                map_socket2status = dict(self.poller.poll(timeout=100))
                pull_socket_status = map_socket2status.get(self.solver2target_pull_socket, None)
                if pull_socket_status is not None and pull_socket_status == zmq.POLLIN: 
                    solver_plain_message:Tuple[str, SolverDataResponse] = self.solver2target_pull_socket.recv_pyobj()
                    solver_service_name, solver_data_response = solver_plain_message
                    if isinstance(solver_data_response.response_content, SolverDoneResponse):
                        solver_task_response:SolverDoneResponse = solver_data_response.response_content
                        if solver_task_response.response_type in [TaskStatus.FAILED, TaskStatus.DONE]:
                            task_response_data = solver_task_response.response_content
                            
                            if task_response_data.task_id not in self.cache_responses:
                                self.cache_responses[task_response_data.task_id] = {}
                            if task_response_data.topic not in self.cache_responses[task_response_data.task_id]:
                                self.cache_responses[task_response_data.task_id][task_response_data.topic] = {}
                            
                            cache_target_response_map = self.cache_responses[task_response_data.task_id][task_response_data.topic]
                            if cache_target_response_map is not None and isinstance(cache_target_response_map, dict):
                                cache_target_response_map.update({
                                    solver_service_name: 
                                    (
                                        solver_task_response.response_type, 
                                        task_response_data.response_content
                                    )
                                })
                            nb_responses = nb_responses + 1 
                            
                        # a response was collected from remote solvers 
                # end if incoming events for solver2target_pull_socket 

                pair_socket_status = map_socket2status.get(self.source_target_pair_socket, None)
                if pair_socket_status is not None and pair_socket_status == zmq.POLLIN:
                    source_plain_message:SourceDataResponse = self.source_target_pair_socket.recv_pyobj()  # message from source 
                    if isinstance(source_plain_message.response_content, SourceDoneResponse):
                        task_response_data = source_plain_message.response_content.response_content
                        if task_response_data.task_id not in self.cache_responses:
                            self.cache_responses[task_response_data.task_id] = {}
                        if task_response_data.topic not in self.cache_responses[task_response_data.task_id]:
                            self.cache_responses[task_response_data.task_id][task_response_data.topic] = {}
                        self.cache_responses[task_response_data.task_id][task_response_data.topic] = task_response_data.response_content
                    if isinstance(source_plain_message.response_content, SourceBusyResponse):
                        logger.debug(f'target has collected {nb_responses:4d} responses')
                        self.source_target_pair_socket.send_pyobj(
                            TargetDataResponse(
                                response_content=TargetFreeResponse(
                                    nb_collected_responses=nb_responses
                                )
                            )
                        )
                        nb_responses = 0 
                    if isinstance(source_plain_message.response_content, SourceFreeResponse):
                        self.target2server_push_socket.send_pyobj(
                            self.cache_responses
                        )
                        logger.debug('target has sent the response to the server')
                        self.cache_responses = {}
                        nb_responses = 0
            except Exception as e:
                logger.error(e)
                keep_loop = False     
        # end while loop 

        return 0 
        
    def __enter__(self):
        try:
            self.solver2target_pull_socket:zmq.Socket = self.ctx.socket(zmq.PULL)
            self.solver2target_pull_socket.bind(self.solver2target_pull_address)

            self.source_target_pair_socket:zmq.Socket = self.ctx.socket(zmq.PAIR)
            self.source_target_pair_socket.connect(self.source_target_pair_address)

            self.target2server_push_socket:zmq.Socket = self.ctx.socket(zmq.PUSH)
            self.target2server_push_socket.connect(self.target2server_pull_address)

            self.poller = zmq.Poller()
            self.poller.register(self.solver2target_pull_socket, zmq.POLLIN)
            self.poller.register(self.source_target_pair_socket, zmq.POLLIN)

            self.sockets_initialization = True 
        except Exception as e:
            logger.error(e) 
        
        return self 

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()
        
        if self.sockets_initialization:
            self.poller.unregister(self.source_target_pair_socket)
            self.poller.unregister(self.solver2target_pull_socket)

            self.solver2target_pull_socket.close()
            self.source_target_pair_socket.close()
            self.target2server_push_socket.close()
            
            logger.debug('target has remove its zeromq sockets')

        return True 