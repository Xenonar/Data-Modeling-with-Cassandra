import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster
from prettytable import PrettyTable

def process_csv(path):
    """
    Description: This function read all the csv file from event_data folder into event_datafile_new.csv
    
    Arguments:
        path: folder name
        
    Returns:
        None
    """

    filepath = os.getcwd() + path

    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))
    full_data_rows_list = [] 
    for f in file_path_list:

        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)    
            for line in csvreader:
                full_data_rows_list.append(line) 

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16])) 
            
def check_numrow(file):
    """
    Description: This function for checking the row in csv file
    
    Arguments:
        file: csv file name
        
    Returns:
        None
    """

    with open(file, 'r', encoding = 'utf8') as f:
        print(sum(1 for line in f))
        
def create_keyspace(name):
    """
    Description: This function create the keyspace for Cassandra using the specific name user provide
    
    Arguments:
        name: the name for keyspace
        
    Returns:
        None
    """
    query = "CREATE KEYSPACE IF NOT EXISTS " + name +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
    try:
        session.execute(query)
    except Exception as e:
        print(e)
    try:
        session.set_keyspace(name)
    except Exception as e:
        print(e)

def create_data(table, primary,opt):
    """
    Description: This function create the table according to the name and Primary key
    
    Arguments:
        table: the name for table
        primary: the primary key for table
        opt: the option must be select 1-3 according to the SELECT query
        
    Returns:
        None
    """
    head = "DROP TABLE IF EXISTS " + table
    session.execute(head)        
    query = "CREATE TABLE IF NOT EXISTS " + table
    if opt==1:
        query = query + "(sessionid text, iteminsession text, artist_name text, song_title text,song_length text, user_firstname text, user_lastname text, userid text,\
        PRIMARY KEY ("+primary+" ))"
    elif opt==2:
        query = query + "(sessionid text, userid text, iteminsession text, artist_name text, song_title text,song_length text, user_firstname text, user_lastname text, \
        PRIMARY KEY ("+primary+" ))"
    elif opt==3:
        query = query + "(song_title text,user_firstname text, user_lastname text,artist_name text, song_length text, sessionid text, iteminsession text, userid text, \
        PRIMARY KEY ("+primary+" ))"

    try:
        session.execute(query)
        insert_data(table,opt)
    except Exception as e:
        print(e)  

def insert_data(table, opt):
    """
    Description: This function insert all data from .csv file into the table
    
    Arguments:
        table: the name for table
        opt: the option must be select 1-3
        
    Returns:
        None
    """
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        if opt==1:
            for line in csvreader:
                query = "INSERT INTO "+table+"(sessionid,iteminsession,artist_name,song_title, song_length,user_firstname,user_lastname,userid)"
                query = query + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                data_line = (line[-3],line[3],line[0],line[-2],line[5],line[1],line[4],line[-1])
                session.execute(query, data_line)
        elif opt==2:
            for line in csvreader:
                query = "INSERT INTO "+table+"(sessionid,userid,iteminsession,artist_name,song_title, song_length,user_firstname,user_lastname)"
                query = query + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                data_line = (line[-3],line[-1],line[3],line[0],line[-2],line[5],line[1],line[4])
                session.execute(query, data_line)
        elif opt==3:
             for line in csvreader:
                query = "INSERT INTO "+table+"(song_title,user_firstname,user_lastname, artist_name,song_length,sessionid,iteminsession, userid)"
                query = query + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                data_line = (line[-2],line[1],line[4],line[0],line[5],line[-3],line[3],line[-1])
                session.execute(query, data_line)
            
def select_data(table, fwh, opt, select_field) :
    """
    Description: This function select the data according to the table and query
    
    Arguments:
        table: the name for table
        fwh: mean from the WHERE statement
        opt: option for select
        select_field: for select the specific column in SELECT instead of 'SELECT * ' (for cost sufficient)
        
    Returns:
        None
    """
    query = "select "+select_field+" from "+table+" WHERE "+fwh 
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    if opt==1:
        col = PrettyTable(['Artist', 'Song', 'Length'])
        for row in rows:
            col.add_row([row.artist_name, row.song_title, row.song_length])
    elif opt==2:
        col = PrettyTable(['Artist', 'Song', 'User Firstname', 'User Lastname'])
        for row in rows:
            col.add_row([row.artist_name, row.song_title, row.user_firstname, row.user_lastname])
    elif opt==3:
        col = PrettyTable(['User Firstname', 'User Lastname'])
        for row in rows:
            col.add_row([row.user_firstname, row.user_lastname])
    else:
        print('error')
    print(col)

def main():
    """
    Description: The main function
    
    Arguments:
        None
    Returns:
        None
    """
    process_csv('/event_data')
    create_keyspace('cassandrapj')
    print('Preparing Data....')
    create_data('song_playlist_session', 'sessionid, iteminsession',1)
    create_data('song_playlist_user', '(sessionid, userid), iteminsession',2)
    create_data('song_playlist_songtitle', '(song_title), user_firstname,user_lastname, userid',3)
    print('Song Playlist 1st Table')
    select_data('song_playlist_session', "sessionid='338' and iteminsession='4'",1, "artist_name,song_title,song_length")
    print('Song Playlist 2nd Table')
    select_data('song_playlist_user',"sessionid='182' AND userid='10'",2, "artist_name,song_title,user_firstname,user_lastname")
    print('Song Playlist 3rd Table')
    select_data('song_playlist_songtitle',"song_title='All Hands Against His Own'",3,"user_firstname,user_lastname")

if __name__ == "__main__":
    cluster = Cluster()
    session = cluster.connect()
    main()