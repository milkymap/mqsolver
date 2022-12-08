from enum import Enum 
from typing import (
    List, Tuple, Dict, Optional, Any 
)
from pydantic import BaseModel, Field

Topic = str
Topics = List[Topic] 
    
class TaskStatus(str, Enum):
    DONE:str='DONE'
    FAILED:str='FAILED'
    
class GenericTask(BaseModel):
    task_id:str 
    task_content:Any 
    list_of_topics:Topics
    
class SpecializedTask(BaseModel):
    topic:Topic 
    task_id:str 
    task_content:Any 

class TaskResponseData(BaseModel):
    task_id:str
    topic:Topic 
    response_content:Any  

Tasks = List[GenericTask]

TaskCacheResponse = Dict[str, Dict[Topic, Optional[ Dict[str, Tuple[TaskStatus, Any]]] ] ]






