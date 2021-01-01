'''
This python file contains Data_Access class.
This python file can be used as  a python module and can be imported from other files to create objects to access functionalities of Data_Access class .  
'''

'''
IMPORT SECTION :
----------------  '''
import os
import json
import threading
from pathlib import Path
from datetime import datetime

'''
CLASS DECLARATION SECTION :
---------------------------  '''

class Data_Access:
    '''
    CLASS DESCRIPTION :
    -------------------
    => Data Access class's objects can be used to create,read,delete key-value pairs in a datastore file in local system .
    => Other processes ( Client process ) can access this Data Access class for creating objects to perform CRD( create,read,delete ) operations on key-value pairs in datastore file .
    
    CLASS VARIABLE(S) :
    -------------------
    1) access_lock -> thread lock for accessing datastore file

    OBJECT VARIABLES :
    ------------------
    1) data_file_path
    2) default_path

    OBJECT METHODS :
    ----------------
    1) __init__()
    2) create()
    3) read()
    4) delete()
    5) isFileAccessible()
    6) get_FileHandler()
    7) isTTL_Expired()
    8) lock_DataStore_access()
    9) release_DataStore_access_lock()
   10) get_FilteredRecords()
   11) write_to_file()
   
    '''
    access_lock = threading.Lock() # access_lock (class variable - static for all objects of this class) - used to access data store file in multithreaded environment in thread safe manner . 
    
    def __init__( self , path = None ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It is the constructor to instantiate Data_Access class's objects .
        => If filepath is passed it will access that filepath else it uses default path( data_store.dat file under current User's Directory) .

        PARAMETERS :
        ------------
        1) path :(str) ->

        RETURNS :
        ---------
        ( Nothing )
    
        '''
        
        if not isinstance( path , ( type(None) , str ) ) : raise TypeError("DATA TYPE ERROR - Invalid datatype for argument 'path' is provided , 'str' type required .")

        self.default_path = os.path.join( Path.home() , "data_store.dat" )
        self.data_file_path = None
        
        if path is None :
            
            file_handle = open( self.default_path , 'a+' )
            file_handle.close()

            self.data_file_path = self.default_path
            
        elif os.path.isfile( path ) : self.data_file_path = path
        
        else : raise FileNotFoundError( "FILE ERROR - Invalid File Path , Try again with a valid file path or default file path." )
            
    def isFileAccessible( self , file_path = None ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It checks the given file is available to access or already accessed by other process .
        
        PARAMETERS :
        ------------
        1) file_path :(str) ->

        RETURNS :
        ---------
        True :(bool) -> If the given file is available to access .
        False :(bool) -> If the given file is already accessed by other process .
    
        '''
        
        if file_path is None :
            raise FileNotFoundError( "FILE ERROR - Invalid File Path , Give a valid file path or go with default file path." ) 

        else :
            
            try : os.rename( file_path , file_path ) ; return True
            except PermissionError : return False
            
    def get_FileHandler( self ) :
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It checks datastore file's availability and its size .
        => Then it generate and returns file handler for datastore file .
        
        PARAMETERS :
        ------------
        ( Nothing )

        RETURNS :
        ---------
        file_handler :(file object)
    
        '''

        if not self.isFileAccessible( self.data_file_path ) :
            raise PermissionError( "FILE ERROR - DataStore file is being accessed by another process ." )
        
        file_size = os.path.getsize( self.data_file_path )
        _1GB = 1073741824
        assert file_size < _1GB , "FILE ERROR - Given file size is more than 1GB , It should be less than 1 GB . Try another file as data store ."

        file_handler = None
        file_handler = open( self.data_file_path , 'r+' )
            
        return file_handler

    def isTTL_Expired( self , record_created_time , time_to_live ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It checks whether the of 'Time to Live' property is expired or not .
        
        PARAMETERS :
        ------------
        1) record_created_time :(str) ->
        2) time_to_live :(str) ->

        RETURNS :
        ---------
        True :(bool) -> If 'Time To Live' property is expired  .
        False :(bool) -> If 'Time To Live' property not expired .
    
        '''
        record_created_time = record_created_time.strip()
        
        formated_record_created_time = datetime.strptime( record_created_time , "%Y%m%d%H%M%S" )
        
        formated_current_time = datetime.strptime( datetime.now().strftime( "%Y%m%d%H%M%S" ) , "%Y%m%d%H%M%S")
        
        if ( formated_current_time - formated_record_created_time ).seconds >= time_to_live : return True
        else : return False

    def lock_DataStore_access( self ) :
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It locks accessibility of datastore file .
        
        PARAMETERS :
        ------------
        ( Nothing )

        RETURNS :
        ---------
        ( Nothing )
    
        '''
        
        while Data_Access.access_lock.locked() : continue
        
        Data_Access.access_lock.acquire()

    def release_DataStore_access_lock( self ) :
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It releases accesibility lock of datastore file .
        
        PARAMETERS :
        ------------
        ( Nothing )

        RETURNS :
        ---------
        ( Nothing )
    
        '''
        Data_Access.access_lock.release()
    
    def get_FilteredRecords( self , file_handler ) :
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It filters all the 'Time To Live' property expired key-value pairs from the datastore file's records .
        
        PARAMETERS :
        ------------
        file_handler :(file object)

        RETURNS :
        ---------
        filtered_records :(list) -> List of filtered records .

        INNER FUNCTION(S) :
        -------------------
        1) filter_timedOuts() -> FUNCTION DESCRIPTION => Checks whether the of 'Time to Live' property is expired or not for a single record .
                                 PARAMERTERS => record :(list)
                                 RETURNS => True :(bool) -> If 'Time To Live' property is not expired for the given record .
                                            False :(bool) -> If 'Time To Live' property expired for the given record .
                                            
        '''
        
        def filter_timedOuts( record ):
            time_to_live = int(record[2])
            record_created_time = record[3]
            if time_to_live == -1 : return True
            else : return not self.isTTL_Expired( record_created_time , time_to_live )
            
        records = file_handler.readlines()
        records = [ record.split('\t') for record in records ]            
        filtered_records = list( filter( filter_timedOuts , records ) )
        
        return filtered_records
    
    def write_to_file( self , record_lines , file_handler ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It writes all the records in the given list into the file mentioned in given file handler .
        
        PARAMETERS :
        ------------
        1) record_lines :(list) -> list of records .
        2) file_handler :(file object) 
        
        RETURNS :
        ---------
        ( Nothing )
    
        '''
        
        file_handler.seek( 0 , 0 )
        file_handler.truncate( 0 )

        file_handler.writelines( record_lines )
        

    def create( self , key , value , ttl = -1 ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It creates a new record in datastore file for the given key-value pair .
        
        PARAMETERS :
        ------------
        1) key :(str) ->
        2) value :(json object - in python , dict datatype acts as json object representation) ->
        3) ttl :(int) -> 'Time to Live' count in seconds for given key-value pair .
                         DEFAULT VALUE : -1 indicates infinity seconds .

        RETURNS :
        ---------
        ( Nothing )
    
        '''

        if not isinstance( key , str ) : raise TypeError("DATA TYPE ERROR - Invalid datatype for argument 'key' is provided , 'str' type is expected .")
        if not isinstance( value , dict ) : raise TypeError("DATA TYPE ERROR - Invalid datatype for argument 'value' is provided , 'dict' type expected .")
        if not isinstance( ttl , ( type(None) , int ) ) : raise TypeError("DATA TYPE ERROR - Invalid datatype for argument 'ttl' is provided , 'int' type expected .")
        
        key = key.strip()
        value = json.dumps( value )
        ttl = str( ttl )
        
        self.lock_DataStore_access()
        
        try :
            file_handler = self.get_FileHandler()
            try :
                filtered_records = self.get_FilteredRecords( file_handler )
            
                isExistingKey = False
                for record in filtered_records :
                    if key == record[0] : isExistingKey = True ; break

                if isExistingKey :
                    raise KeyError( "KEY ERROR - Given key already exists ." )
            
                current_timeObj = datetime.now()
                current_time = str( current_timeObj.strftime( "%Y%m%d%H%M%S" ) )
                
                filtered_record_lines = list( map( lambda record : "\t".join(record) , filtered_records) )
                new_record = "\t".join( [ key , value , ttl , current_time ] )+'\n'
                filtered_record_lines.append( new_record )
                
                self.write_to_file( filtered_record_lines , file_handler )
                
            finally : file_handler.close()
        
        finally : self.release_DataStore_access_lock()

    def read( self , key ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It searchs value for given key in datastore file and returns that value .
        
        PARAMETERS :
        ------------
        1) key :(str) ->

        RETURNS :
        ---------
        1) value :(json object - in python , dict datatype acts as json object representation)
    
        '''

        if not isinstance( key , str ) : raise TypeError("DATA TYPE ERROR - Invalid datatype for argument 'key' is provided , 'str' type expected .")
        
        key = key.strip()
        
        self.lock_DataStore_access()
        
        try :
            file_handler = self.get_FileHandler()
            try :
                filtered_records = self.get_FilteredRecords( file_handler )
        
                filtered_record_lines = list( map( lambda record : "\t".join(record) , filtered_records) )
                self.write_to_file( filtered_record_lines , file_handler )
        
                req_record = None
                for record in filtered_records :
                    if key == record[0] : req_record = record ; break
            
                if req_record is not None : value = json.loads( record[1] ) ; return value
                else : raise KeyError( "KEY ERROR - Record for given key does not exist" )
                
            finally : file_handler.close()
            
        finally : self.release_DataStore_access_lock()
        
    def delete( self , key = None ):
        '''
        FUNCTION DESCRIPTION :
        ----------------------
        => It searchs record for the given key in datastore file and removes that record .
        
        PARAMETERS :
        ------------
        1) key :(str) ->

        RETURNS :
        ---------
        ( Nothing )
    
        '''

        if not isinstance( key , str ) : raise TypeError("DATA TYPE ERROR - Invalid datatype for argument 'key' is provided , 'str' type expected .")
        
        key = key.strip()
        
        self.lock_DataStore_access()
        
        try :
            file_handler = self.get_FileHandler()
            try :
                filtered_records = self.get_FilteredRecords( file_handler )
        
                keyExists = False
                for record_index in range( len( filtered_records ) ) :
                    record = filtered_records[ record_index ]
                    if key == record[0] : keyExists = True ; filtered_records.pop(record_index) ; break
            
                if not keyExists : raise KeyError( "KEY ERROR - Record for given key does not exist ." )

                filtered_record_lines = list( map( lambda record : "\t".join( record ) ,filtered_records ) )
                self.write_to_file( filtered_record_lines , file_handler )

            finally : file_handler.close()
            
        finally : self.release_DataStore_access_lock()

        
