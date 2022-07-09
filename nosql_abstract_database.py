"""
Created on Fri Sept 4 09:39 2020

@author: Alejandro Palomino
"""
from abc import ABC, abstractclassmethod
from typing import Dict, List, Union, Dict, Set, Generic, TypeVar, Any
from json import load
from os import environ

T = TypeVar("T")

class Abstract_Database(ABC, Generic[T]):

    _ip: str
    _port: int
    _username: str
    _password: str
    _db_name: str

    def __init__(self, config_file: Union[str, None]):
        """
        If path to config file not specify, the system will be read the values from the os variable system.
        - config_file (str): Path to the configuration file necessary to connect to the database server
        """
        if config_file is not None:
            with open(config_file) as f:
                json_file = load(f)
                self._ip = json_file["ip"]
                self._port = json_file["port"]
                self._username = json_file["username"]
                self._password = json_file["password"]
                self._db_name = json_file["db_name"]
        else:
            self._ip = environ["ip"]
            self._port = environ["port"]
            self._username = environ["username"]
            self._password = environ["password"]
            self._db_name = environ["db_name"]

    def get_db_name(self) -> str:
        return self._db_name

    def set_db_name(self, db_name):
        self._db_name = db_name

    @abstractclassmethod
    def _connect(self) -> Any: pass

    @abstractclassmethod
    def _close_connection(self):  pass

    @abstractclassmethod
    def select(self): pass

    @abstractclassmethod
    def select_one(self, query: Dict, collection: str, fields: Set[str] = None) -> Union[T, None]: pass

    @abstractclassmethod
    def insert_one(self, data): pass

    @abstractclassmethod
    def insert_many(self, datas: List[Dict]): pass

    @abstractclassmethod
    def count(self, query: Dict, collection: str, fields: Set[str] = None) -> int: pass

    @abstractclassmethod
    def update_one(self, query: Dict, new_values: Dict[str, Any], collection: str) -> bool: pass

    @abstractclassmethod
    def find_last(self, sort_field: str, collection: str, fields: Set[str] = None) -> Union[T, None]: pass

    @abstractclassmethod
    def delete_one(self, query : Dict[str, Any], collection: str) -> bool: pass

    @abstractclassmethod
    def delete_many(self, query : Dict[str, Any], collection: str) -> int: pass
