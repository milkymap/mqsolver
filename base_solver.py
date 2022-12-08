
from schema.task import SpecializedTask, TaskStatus, TaskResponseData
from schema.worker import SolverDoneResponse, SolverDataResponse
from typing import Any

from abc import ABC, abstractmethod

class ABCSolver(ABC):
    """abstract base class solver"""
    def __init__(self) -> None:
        # simple initialization 
        # option assignment etc ...!
        # use initialize() for heavy config such as : database connection or model loading
        pass
    
    @abstractmethod
    def initialize(self) -> None:
        # heavy initialization 
        # create session
        # database connection
        pass 
    
    @abstractmethod
    def destroy_ressources(self) -> None:
        # use this function to end some contexts such as :
        # openned sessions etc ...! 
        pass 
    
    @abstractmethod
    def process_message(self, task:SpecializedTask ,*args:Any, **kwds:Any) -> Any:
        # this function is the strategy 
        # each solver will use it to process the incoming task
        # it can be a strategy pattern for each registered topics  
        return None   

    def __call__(self, task:SpecializedTask, *args: Any, **kwds: Any) -> SolverDataResponse:
        # special method : turn the class to a callable 
        # WARNING : ***do not overwrite this function***  
        try:
            response = self.process_message(task, *args, **kwds)
            task_status = TaskStatus.DONE 
        except Exception as e:
            response = None 
            task_status = TaskStatus.FAILED
        
        return SolverDataResponse(
            response_content=SolverDoneResponse(
                response_type=task_status,
                response_content=TaskResponseData(
                    task_id=task.task_id,
                    topic=task.topic,
                    response_content=response 
                )
            )
        )             
        
 
