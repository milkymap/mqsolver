import cv2 
import numpy as np 

from urllib.request import urlopen

from os import path 
from hashlib import sha256, md5

from typing import Any 

from base_solver import ABCSolver
from schema.task import SpecializedTask

from PIL import Image 
from sentence_transformers import SentenceTransformer 

from log import logger 

class ImageProcessingSolver(ABCSolver):
    def __init__(self, path2target_dir:str) -> None:
        super().__init__()
        self.path2target_dir = path2target_dir

    
    def initialize(self) -> None:
        # load deeep learning model 
        # make data base connection
        return super().initialize()
    
    def destroy_ressources(self) -> None:
        return super().destroy_ressources()
    
    def process_message(self, task: SpecializedTask, *args: Any, **kwds: Any) -> Any:
        topic = task.topic 
        task_id = task.task_id
        path2source_image = task.task_content
        _, filename = path.split(path2source_image)

        bgr_image = cv2.imread(path2source_image, cv2.IMREAD_COLOR)
        if topic == 'RESIZE':
            resized_image = cv2.resize(bgr_image, (512, 512))
            path2resized_image = path.join(self.path2target_dir, f'resized_{filename}')
            print(path2resized_image)
            cv2.imwrite(path2resized_image, resized_image)
            return path2resized_image

        if topic == 'TOGRAY':
            gray_image = cv2.cvtColor(bgr_image, cv2.COLOR_BGR2GRAY)
            path2gray_image = path.join(self.path2target_dir, f'gray_{filename}')
            cv2.imwrite(path2gray_image, gray_image)

            return path2gray_image 
        

class ImageEmbeddingSolver(ABCSolver):
    def __init__(self, model_name:str, cache_dir:str) -> None:
        super().__init__()
        self.model_name = model_name 
        self.cache_dir = cache_dir 
    
    def initialize(self) -> None:
        self.vectorizer = SentenceTransformer(
            model_name_or_path=self.model_name,
            cache_folder=self.cache_dir
        )

    def destroy_ressources(self) -> None:
        return super().destroy_ressources()

    def process_message(self, task: SpecializedTask, *args: Any, **kwds: Any) -> np.ndarray:
        path2image = task.task_content
        image = Image.open(path2image)
        embedding = self.vectorizer.encode(image, device='cuda:0')
        return embedding