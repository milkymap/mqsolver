
from enum import Enum 

class SignalValue(bytes, Enum):
    IS_ALIVE:bytes=b'IS_ALIVE'
    IS_READY:bytes=b'IS_READY'
    QUITLOOP:bytes=b'QUITLOOP'