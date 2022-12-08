from enum import Enum 
from typing import Union, Literal
from pydantic import BaseModel, Field
from schema.task import Topics, TaskStatus, TaskResponseData

# change response per message 

class WorkerStatus(bytes, Enum):
    FREE:bytes=b'FREE'
    JOIN:bytes=b'JOIN'
    DONE:bytes=b'DONE'
    BUSY:bytes=b'BUSY'

# switch schema 
class SwitchJoinResponse(BaseModel):
    type_:Literal[WorkerStatus.JOIN]=WorkerStatus.JOIN
    switch_id:int 
    list_of_topics:Topics 

class SwitchFreeResponse(BaseModel):
    type_:Literal[WorkerStatus.FREE]=WorkerStatus.FREE
    switch_id:int 
     
class SwitchBusyResponse(BaseModel):
    type_:Literal[WorkerStatus.BUSY]=WorkerStatus.BUSY
    switch_id:int 
     
class SwitchDoneResponse(BaseModel):
    type_:Literal[WorkerStatus.DONE]=WorkerStatus.DONE
    switch_id:int  

# source schema 
class SourceJoinResponse(BaseModel):
    type_:Literal[WorkerStatus.JOIN]=WorkerStatus.JOIN 
    
class SourceFreeResponse(BaseModel):
    type_:Literal[WorkerStatus.FREE]=WorkerStatus.FREE
    
class SourceBusyResponse(BaseModel):
    type_:Literal[WorkerStatus.BUSY]=WorkerStatus.BUSY 
    
class SourceDoneResponse(BaseModel):
    type_:Literal[WorkerStatus.DONE]=WorkerStatus.DONE 
    response_content:TaskResponseData 

# target schema 
class TargetJoinResponse(BaseModel):
    type_:Literal[WorkerStatus.JOIN]=WorkerStatus.JOIN 
     
class TargetFreeResponse(BaseModel):
    type_:Literal[WorkerStatus.FREE]=WorkerStatus.FREE
    nb_collected_responses:int  

class TargetBusyResponse(BaseModel):
    type_:Literal[WorkerStatus.BUSY]=WorkerStatus.BUSY

class TargetDoneResponse(BaseModel):
    type_:Literal[WorkerStatus.DONE]=WorkerStatus.DONE 

# solver schema 
class SolverJoinResponse(BaseModel):
    type_:Literal[WorkerStatus.JOIN]=WorkerStatus.JOIN 
    solver_id:int  

class SolverFreeResponse(BaseModel):
    type_:Literal[WorkerStatus.FREE]=WorkerStatus.FREE
    solver_id:int
    switch_id:int   

class SolverDoneResponse(BaseModel):
    type_:Literal[WorkerStatus.DONE]=WorkerStatus.DONE 
    response_type:TaskStatus
    response_content:TaskResponseData 

    class Config:
        arbitrary_types_allowed = True

class SolverBusyResponse(BaseModel):
    type_:Literal[WorkerStatus.BUSY]=WorkerStatus.BUSY

class SwitchDataResponse(BaseModel):
    response_content:Union[SwitchJoinResponse, SwitchFreeResponse, SwitchBusyResponse, SwitchDoneResponse]=Field(..., discriminator='type_')

class SourceDataResponse(BaseModel):
    response_content:Union[SourceJoinResponse, SourceFreeResponse, SourceBusyResponse, SourceDoneResponse]=Field(..., discriminator='type_')

class TargetDataResponse(BaseModel):
    response_content:Union[TargetJoinResponse, TargetFreeResponse, TargetBusyResponse, TargetDoneResponse]=Field(..., discriminator='type_')
    
class SolverDataResponse(BaseModel):
    response_content:Union[SolverJoinResponse, SolverFreeResponse, SolverBusyResponse, SolverDoneResponse]=Field(..., discriminator='type_')

    
if __name__ == '__main__':
    M = SolverDataResponse(
        response_type=WorkerStatus.FREE,
        response_content=SolverFreeResponse(solver_id=2, switch_id=1,type_=WorkerStatus.JOIN)
    )

    print(M)