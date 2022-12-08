import click 

import uuid
from typing import List 

from glob import glob 
from os import path 

from time import sleep 

from log import logger 

from strategies import ImageProcessingSolver, ImageEmbeddingSolver
from schema.task import GenericTask, Tasks
from schema.config import WorkerConfig, SwitchConfig

from server import CCRServer, PRLServer

@click.command()
@click.option('--path2source_dir', help='path to source files', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--path2target_dir', help='path to source files', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--nb_runners', help='number of runners(process)', type=int, default=2)
def concurrent_runner(path2source_dir:str, path2target_dir:str, nb_runners:int):
    try:    
        filepaths = sorted(glob(path.join(path2source_dir, '*')))
        tasks = []
        for path_ in filepaths:
            task = GenericTask(
                task_id=path_,
                task_content=path_,
                list_of_topics=['RESIZE', 'TOGRAY']
            )
            tasks.append(task)
        
        worker_config = WorkerConfig(
            list_of_switch_configs=[
                SwitchConfig(
                    service_name='image-processing',
                    solver_factory=lambda: ImageProcessingSolver(path2target_dir),
                    nb_solvers=1,
                    list_of_topics=['RESIZE', 'TOGRAY']
                )
            ],
            max_nb_running_tasks=256
        )

        server = CCRServer(
            worker_config=worker_config,
            nb_runners=nb_runners
        )

        with server as srv:
            srv.start_all_runners()
            sleep(1)
            srv.submit_tasks(tasks[:4096])


    except Exception as e:
        logger.error(e)    


@click.command()
@click.option('--path2source_dir', help='path to source files', type=click.Path(exists=True, file_okay=False, dir_okay=True))
def parallel_runner(path2source_dir:str):
    try:    
        worker_config = WorkerConfig(
            list_of_switch_configs=[
                SwitchConfig(
                    service_name='image-embedding',
                    solver_factory=lambda: ImageEmbeddingSolver('clip-ViT-B-32', cache_dir='transformers'),
                    nb_solvers=2,
                    list_of_topics=['EMBEDDING']
                )
            ],
            max_nb_running_tasks=512
        )

        filepaths = sorted(glob(path.join(path2source_dir, '*')))
         
        
        tasks:List[GenericTask] = []
    
        for path2image in filepaths:
            topics = ['EMBEDDING']
            task = GenericTask(
                task_id=str(uuid.uuid4()), 
                list_of_topics=topics,
                task_content=path2image
            )

            tasks.append(task)
        
        server_ = PRLServer(worker_config)

        with server_ as srv:
            srv.start_all_runners()
            sleep(2)
            response = srv.submit_tasks(tasks=tasks[:4096])

            
            #print(response)


    except Exception as e:
        logger.error(e)    
