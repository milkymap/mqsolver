from zmq import Context
from threading import Event
from typing import Protocol, Callable

from base_solver import ABCSolver
from engine.solver import CCRSolver, PRLSolver
from schema.zmqconfig import FSTConfig, SNDConfig

class SolverStartingProtocol(Protocol):
    def start_solver(self, ctx:Context, shutdown_signal:Event, solver_id:int, switch_id:int, service_name:str, solver_factory:Callable[..., ABCSolver], fst_address_settings:FSTConfig, snd_address_settings:SNDConfig):
        ...

class PRLSolverStarter:
    def __init__(self):
        pass 

    def start_solver(self, ctx:Context, shutdown_signal:Event, solver_id:int, switch_id:int, service_name:str, solver_factory:Callable[..., ABCSolver], fst_address_settings:FSTConfig, snd_address_settings:SNDConfig):
        solver_ = PRLSolver(
            solver_id=solver_id,
            switch_id=switch_id,
            service_name=service_name,
            solver_factory=solver_factory,
            fst_address_settings=fst_address_settings,
            snd_address_settings=snd_address_settings
        )

        with solver_ as slv:
            slv.start_loop()

class CCRSolverStarter:
    def __init__(self):
        pass 

    def start_solver(self, ctx:Context, shutdown_signal:Event, solver_id:int, switch_id:int, service_name:str, solver_factory:Callable[..., ABCSolver], fst_address_settings:FSTConfig, snd_address_settings:SNDConfig):
        solver_ = CCRSolver(
            ctx=ctx,
            solver_id=solver_id,
            switch_id=switch_id,
            service_name=service_name,
            solver_factory=solver_factory,
            shutdown_signal=shutdown_signal,
            fst_address_settings=fst_address_settings,
            snd_address_settings=snd_address_settings
        )

        with solver_ as slv:
            slv.start_loop()
    