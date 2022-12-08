import zmq 
import pickle 

from threading import Event
from time import time, sleep 

from log import logger 
from schema.config import SwitchConfig
from schema.worker import SwitchDataResponse, SolverDataResponse, SwitchJoinResponse, SolverJoinResponse
from schema.signal import SignalValue
from schema.zmqconfig import FSTConfig, SNDConfig

class ZMQSwitch:
    def __init__(
        self, 
        ctx:zmq.Context,
        switch_id:int,
        switch_config:SwitchConfig,
        shutdown_signal:Event,  
        fst_address_settings:FSTConfig,
        snd_address_settings:SNDConfig
        ):


        self.ctx = ctx 

        self.switch_id = switch_id 
        self.switch_config = switch_config
        self.shutdown_signal = shutdown_signal
        
        self.source_switch_router_address = fst_address_settings.SOURCE_SWITCH_ROUTER
        self.source2switch_publisher_address = fst_address_settings.SOURCE2SWITCH_PUBLISHER

        self.switch2solver_push_addresss = snd_address_settings.SWITCH2SOLVER_PUSH
        self.switch_solver_router_address = snd_address_settings.SWITCH_SOLVER_ROUTER
        self.switch2solver_publisher_address = snd_address_settings.SWITCH2SOLVER_PUBLISHER
        
        self.sockets_initialization:bool = False 
    
    def syncrhonize_with_solvers(self, max_duration:float=10.0):
        start_timer = time()
        counter = 0
        keep_loop = True 
        while keep_loop:
            router_socket_status = self.switch_solver_router_socket.poll(100)
            if router_socket_status == zmq.POLLIN: 
                _, _, solver_encoded_message = self.switch_solver_router_socket.recv_multipart()
                solver_plain_message:SolverDataResponse = pickle.loads(solver_encoded_message)
                if isinstance(solver_plain_message.response_content, SolverJoinResponse):
                    # a solver has joined the group
                    counter = counter + 1
            
            duration = time() - start_timer
            if duration > max_duration or counter == self.switch_config.nb_solvers:
                keep_loop = False 
        # end while loop 

        return self.switch_config.nb_solvers == counter 
    
    def check_signal_from_source(self, signal_value:SignalValue, timeout:int=5000) -> bool:
        subscriber_socket_status = self.source2switch_subscriber_socket.poll(timeout=timeout)
        if subscriber_socket_status == zmq.POLLIN: 
            incoming_topic, _ = self.source2switch_subscriber_socket.recv_multipart()
            if incoming_topic == signal_value:
                return True   
        return False  

    def start_loop(self) -> int:
        if not self.sockets_initialization:
            logger.debug(f'switch {self.switch_id:03d} was not able to create its sockets')
            return 1 
        
        logger.debug(f'switch {self.switch_id:03d} has created its sockets')
        
        synchronize_status = self.syncrhonize_with_solvers()
        if not synchronize_status:
            logger.warning(f'switch {self.switch_id:03d} was not able to make syncrhonization with solvers')
            return 1 
        
        logger.debug(f'switch {self.switch_id:03d} has performed the syncrhonization with the solvers')
        
        self.source_switch_dealer_socket.send_string('', flags=zmq.SNDMORE)
        self.source_switch_dealer_socket.send_pyobj(
            SwitchDataResponse( 
                response_content=SwitchJoinResponse(
                    switch_id=self.switch_id,
                    list_of_topics=self.switch_config.list_of_topics
                )
            )
        )

        readyness_signal = self.check_signal_from_source(SignalValue.IS_READY, 5000)
        if not readyness_signal:
            logger.warning(f'switch {self.switch_id:03d} has waited too long to get (readyness signal) from the source')
            return 1  
        
        logger.debug(f'switch {self.switch_id:03d} has gotten the (readyness signal) from the source')

        # notify solver to start their event loop 
        self.switch2solver_publisher_socket.send_multipart([SignalValue.IS_READY, b''])

        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                break 

            try:
                subscriber_socket_status = self.source2switch_subscriber_socket.poll(timeout=100) 
                if subscriber_socket_status is not None and subscriber_socket_status == zmq.POLLIN:
                    _, source_encoded_message = self.source2switch_subscriber_socket.recv_multipart()
                    self.switch2solver_push_socket.send(source_encoded_message)
                    logger.debug(f'switch {self.switch_id:03d} has send the task to the remote solvers')
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop 

        # notify solver to start their event loop 
        logger.debug(f'switch {self.switch_id:03d} has exited its loop and will leave the source')
        return 0 


    def __enter__(self):
        try:
          
            self.source2switch_subscriber_socket:zmq.Socket = self.ctx.socket(zmq.SUB)

            self.source2switch_subscriber_socket.setsockopt(zmq.SUBSCRIBE, SignalValue.IS_ALIVE)
            self.source2switch_subscriber_socket.setsockopt(zmq.SUBSCRIBE, SignalValue.IS_READY)
            self.source2switch_subscriber_socket.setsockopt(zmq.SUBSCRIBE, SignalValue.QUITLOOP)

            for topic in self.switch_config.list_of_topics:
                self.source2switch_subscriber_socket.setsockopt_string(zmq.SUBSCRIBE, topic)

            self.source2switch_subscriber_socket.connect(self.source2switch_publisher_address)

            self.source_switch_dealer_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            self.source_switch_dealer_socket.connect(self.source_switch_router_address)

            self.switch_solver_router_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            self.switch_solver_router_socket.bind(self.switch_solver_router_address)

            self.switch2solver_publisher_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
            self.switch2solver_publisher_socket.bind(self.switch2solver_publisher_address)
            
            self.switch2solver_push_socket:zmq.Socket = self.ctx.socket(zmq.PUSH)
            self.switch2solver_push_socket.bind(self.switch2solver_push_addresss)

            self.sockets_initialization = True  
        except Exception as e:
            logger.error(e)
        
        return self 
    
    def __exit__(self, exc_type, exc_value, traceback):
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()

        if self.sockets_initialization:
            self.switch2solver_push_socket.close()
            self.switch_solver_router_socket.close()
            self.source_switch_dealer_socket.close()
            self.switch2solver_publisher_socket.close()
            self.source2switch_subscriber_socket.close()
            logger.debug(f'switch {self.switch_id:03d} has realsed its sockets')

        return True 

    
