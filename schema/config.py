
from enum import Enum

from typing import List, Callable 
from pydantic import BaseModel
from base_solver import ABCSolver

from schema.task import Topics

class RUNNER_MODE(str, Enum):
    CONCURRENT:str='CONCURRENT'
    PARALLEL:str='PARALLEL'

class SwitchConfig(BaseModel):
    service_name:str 
    solver_factory:Callable[..., ABCSolver]
    nb_solvers:int 
    list_of_topics:Topics
    
    class Config:
        arbitrary_types_allowed = True
    
class WorkerConfig(BaseModel):
    max_nb_running_tasks:int 
    list_of_switch_configs:List[SwitchConfig] 
    
    class Config:
        arbitrary_types_allowed = True