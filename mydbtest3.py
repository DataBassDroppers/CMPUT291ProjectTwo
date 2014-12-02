import bsddb3 as bsddb
import database
import random
import time
import sys
import os

DA_FILE_B = "/tmp/my_db/btree_db"
DA_FILE_H = "/tmp/my_db/hashtable_db"
DA_FILE_IB = "/tmp/my_db/indexfile_btree_db"
DA_FILE_IRB = "/tmp/my_db/indexfile_rbtree_db"
DB_SIZE = 1000
SEED = 10000000




def main():
    
    if len(sys.argv) != 2:
        print("Invalid number of arguments, expected 1 but received " + str(len(sys.argv) - 1))
        return 0
    tmp = ["btree", "hash", "indexfile"]
    dbType = sys.argv[1]
    if dbType not in tmp:
        print("Invalid argument, should be 'btree', 'hash', or 'indexfile'")
        return 0
    
    cont = True
    while cont:
    
        go = True
        while go:
            ans, go = menu()    
    
        # Create Database
        if ans == 1:
            data = database.getData() 
            db = database.makeDB(dbType,data)
            print("----Syncing to disk----")
        
            if type(db) == tuple:
                db[0].sync()
                db[1].sync()
                db[2].sync()
            else:
                db.sync()
            print("Sync complete.\n")
            
        # Perform Key Search    
        elif ans == 2:
            db=database.openDB(dbType)

            key= input("Please enter key: ")
            key = key.encode(encoding='UTF-8')

            if type(db) == tuple:
                database.keySearch(db[0], key)
            else:
                database.keySearch(db, key)
                
        # Perform DataSearch        
        elif ans == 3:
            db=database.openDB(dbType)

            value = input("Please enter a data value: ")
            value = value.encode(encoding='UTF-8')

            if type(db) == tuple:
                database.dataSearch(db[1], dbType, value)
            else:
                database.dataSearch(db, dbType, value)
                
        # Perform Range Search        
        elif ans == 4:
            db=database.openDB(dbType)

            lower = input("Please enter the start of the range: ")
            upper = input("Please enter the end of the range: ")
            lower = lower.encode(encoding='UTF-8')
            upper = upper.encode(encoding='UTF-8')
            
            if type(db) == tuple:
                database.rangeSearchBTree(db[2], lower, upper)
            elif dbType == 'btree':
                database.rangeSearchBTree(db, lower, upper)
            elif dbType == 'hash':
                database.rangeSearchHash(db, lower, upper)
                
        # Destroy Database
        elif ans == 5:
            database.destroyDB(dbType)
            
        # Quit program
        elif ans == 6:
            cont = False

    try:
        db.close()
    except:
        pass



def menu():
    print()
    print("[1] Create and populate a database.")
    print("[2] Retrieve records with a given key")
    print("[3] Retrieve records with a given data")
    print("[4] Retrieve records with a given range of key values")
    print("[5] Destroy the database")
    print("[6] Quit")
    ans = input("Enter an option: ")
    try:
        tmp = int(ans)
    except:
        print("Non-numerical input.")
        return None, True
    
    if tmp > 0 and tmp < 7:
        return tmp, False
    else:
        print("Enter an option within range.")
        return None, True


    
if __name__ == "__main__":
    main()

