"""
Created on Fri Sept 4 10:01 2020

@author: Alejandro Palomino
"""

from pymongo.cursor import Cursor
from pymongo import ASCENDING as ASC, DESCENDING as DESC
from pymongo import MongoClient
from pymongo.collection import Collection
from .nosql_abstract_database import Abstract_Database
from typing import Any, Dict, List, Set, Tuple, Union

ASCENDING = ASC
DESCENDING = DESC

class MyCursor:

    __client: MongoClient
    __cursor: Cursor

    def __init__(self, client: MongoClient, cursor: Cursor):
        self.__client = client
        self.__cursor = cursor

    def __enter__(self):
        #ttysetattr etc goes here before opening and returning the file object
        return self.__cursor

    def __exit__(self, type, value, traceback):
        #Exception handling here
        self.__client.close()
        self.__cursor.close()

class MongoDB(Abstract_Database):

    def __init__(self, config_file: Union[str, None] = None):
        Abstract_Database.__init__(self, config_file)

    def _connect(self) -> MongoClient:
        """ 
        Return a database client -> MongoClient
        """
        client = MongoClient(f"mongodb://{self._username}:{self._password}@{self._ip}:{self._port}")
        return client

    def _close_connection(self, client):
        """
        Close connection from a client

        client -> MongoClient: client that you want to close the connection with the database
        """
        client.close()

    def select_one(self, query: Dict, collection: str, fields: Set[str] = None) -> Union[Dict, None]:
        """
        Return document in dict format if exists, else return None
        """
        success = True
        try:
            #Connect to database
            client = self._connect()
            db = client[self.get_db_name()]

            #Get data from database
            collection: Collection = db[collection]
            doc = collection.find_one(query, fields)
        except:
            success = False
        finally:
            #Close connection
            self._close_connection(client)
            return doc, success

    def select(self, query: Dict, collection: str, fields: Set[str] = None, sort_fields: List[Tuple[str, int]] = [], limit: int = 0) -> MyCursor:
        """ 
        Ejecute query and get the result 

        Parameters
        ----------

        - query (Dict[str, Any]): Query to execute
        - collection (str): collection where get data 
        - fields (Set[str]): specify the fields to get from data, default value get all fields
        - sort_field (List[Tuple[field, direcction]]): List of Tuples where first value is the field to sort and second value the direction, ASCENDING, DESCENDING
        - limit (int): number of max documents to get, default value get all documents

        Result
        ------

        If no error return MyCursor object, otherwise return None

        """
        
        #Connect to database
        client = self._connect()
        db = client[self.get_db_name()]

        #Get data from database
        collection: Collection = db[collection]
        if sort_fields != []:
            cursor = collection.find(query, fields).sort(sort_fields).limit(limit)
        else:
            cursor = collection.find(query, fields).limit(limit)
        myCursor = MyCursor(client, cursor)

        return myCursor


    def insert_one(self, data: Dict[str, Any], collection: str) -> bool:
        """
        Insert new document into database

        Parameters:
        -----------

        - data (Dict[str, Any]): dictionary to insert into database
        - collection (str): collection where insert data 
        """
        # Connect to database
        success = True
        try:
            client = self._connect()
            db = client[self.get_db_name()]

            collection: Collection = db[collection]
            collection.insert_one(data)
        except:
            success = False
            
        finally:
            #Close connection
            self._close_connection(client)
            return success

    def insert_many(self, datas: List[Dict[str, Any]], collection: str):
        """
        Insert multiple documents into database

        Parameters:
        -----------

        - datas (List[Dict[str, Any]]): list of dictionaries to insert into database
        - collection (str): collection where insert data 
        """
        #Connect to database
        try:        
            client = self._connect()
            db = client[self.get_db_name()]
            collection: Collection = db[collection]
            collection.insert_many(datas)
            
        finally:
            #Close connection
            self._close_connection(client)

    def count(self, query: Dict, collection: str) -> int:
        """ 
        Get number of documents which exists from given query, if was an error with the query return -1

        Parameters:
        -----------

        - query (Dict[str, Any]): Query to execute
        - collection (str): collection where get data 
        - fields (Set[str]): specify the fields to get from data, default value get all fields

        Return
        ------

        Number of documents from given query
        """
        count = -1
        try:
            #Connect to database
            client = self._connect()
            db = client[self.get_db_name()]

            #Get data from database
            collection: Collection = db[collection]
            count = collection.count_documents(query)
        finally:
            #Close connection
            self._close_connection(client)
            return count

    def count_all(self, collection: str) -> int:
        """ 
        Get number of documents in the given collection

        Return
        ------

        Number of documents from given collection
        """
        count = -1
        try:
            #Connect to database
            client = self._connect()
            db = client[self.get_db_name()]

            #Get data from database
            collection: Collection = db[collection]
            count = collection.estimated_document_count()
        finally:
            #Close connection
            self._close_connection(client)
            return count

    def update_one(self, query: Dict, new_values: Dict[str, Any], collection: str) -> bool:
        """
        Update a single document matching the query

        Parameters
        ----------

        - query: Query to match document to update
        - new_values: Dictionary that contain the keys of the document to update and the new values for each key
        """
        #Connect to database
        modified_count = 0
        try:
            client = self._connect()
            db = client[self.get_db_name()]

            #Get data from database
            collection: Collection = db[collection]
            r = collection.update_one(query, {"$set" : new_values})
            modified_count = r.modified_count
        finally:
            #Close connection
            self._close_connection(client)
            return modified_count > 0      

    def find_last(self, sort_field: str, collection: str, fields: Set[str] = None) -> Union[Dict, None]:
        """
        Return last document base on sort_field if exists, else return None

        Parameters:
        -----------

        - sort_field (str): field to sort
        - collection (str): collection where get data 
        - fields (Set[str]): specify the fields to get from data, default value get all fields
        """
        #Connect to database
        try:
            client = self._connect()
            db = client[self.get_db_name()]

            #Get data from database
            collection: Collection = db[collection]
            cursor = collection.find({}, fields).sort([(sort_field, DESCENDING)]).limit(1)
            last = (list(cursor))[0]
        
        finally:
            #Close connection
            self._close_connection(client) 
            
        return last

    def delete_one(self, query : Dict[str, Any], collection: str) -> bool:
        """
            Deletes the document matched with the query and returns success of the operation

            Parameters:
            -----------

            - query (Dict[str, Any]): Query to execute and delete first document from the result
            - collection (str): collection where get data 
        """
        deleted_count = 0
        #Connect to database
        try:
            client = self._connect()
            db = client[self.get_db_name()]

            #Delete entries from database
            collection: Collection = db[collection]
            if query:
                r = collection.delete_one(query)
                deleted_count = r.deleted_count
        
        finally:
            #Close connection
            self._close_connection(client)
            return deleted_count > 0

    def delete_many(self, query : Dict[str, Any], collection: str) -> int:
        """"
            Deletes all documents matched with the query and returns number of documents deleted

            Parameters:
            -----------

            - query (Dict[str, Any]): Query to execute and delete all documents from the result
            - collection (str): collection where get data 
        """
        deleted_count = 0
        #Connect to database
        try:
            client = self._connect()
            db = client[self.get_db_name()]

            #Delete entries from database
            collection: Collection = db[collection]

            if query:
                r = collection.delete_many(query)
                deleted_count = r.deleted_count
        finally:
            #Close connection
            self._close_connection(client)
            return deleted_count
