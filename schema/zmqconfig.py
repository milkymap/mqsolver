from pydantic import BaseModel

class FSTConfig(BaseModel):
    SERVER2SOURCE_PUSH:str='ipc://server2source_push.ipc'
    TARGET2SERVER_PULL:str='ipc://target2server_pull.ipc'
    
    SOLVER2TARGET_PULL:str
    
    SOURCE_TARGET_PAIR:str='inproc://source_target_pair'
    SOURCE_SWITCH_ROUTER:str='inproc://source_switch_router'
    SOURCE2SWITCH_PUBLISHER:str='inproc://source2switch_publisher'
    
class SNDConfig(BaseModel):
    SWITCH_ID:int
    SWITCH2SOLVER_PUSH:str 
    SWITCH_SOLVER_ROUTER:str
    SWITCH2SOLVER_PUBLISHER:str 

class CCR_FST_Config(FSTConfig):
    SOLVER2TARGET_PULL:str='inproc://solver2target_pull'

class PRL_FST_Config(FSTConfig):
    SOLVER2TARGET_PULL:str='ipc://solver2target_pull.ipc'

class CCR_SND_Config(SNDConfig):
    SWITCH_ID:int=0
    SWITCH2SOLVER_PUSH:str=f'inproc://switch2solver_push_{SWITCH_ID:03d}'
    SWITCH_SOLVER_ROUTER:str=f'inproc://switch_solver_router_{SWITCH_ID:03d}'
    SWITCH2SOLVER_PUBLISHER:str=f'inproc://switch2solver_publisher_{SWITCH_ID:03d}'

class PRL_SND_Config(SNDConfig):
    SWITCH_ID:int=0
    SWITCH2SOLVER_PUSH:str=f'ipc://switch2solver_push_{SWITCH_ID:03d}.ipc'
    SWITCH_SOLVER_ROUTER:str=f'ipc://switch_solver_router_{SWITCH_ID:03d}.ipc'
    SWITCH2SOLVER_PUBLISHER:str=f'ipc://switch2solver_publisher_{SWITCH_ID:03d}.ipc'

