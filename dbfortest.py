import bsddb3 as bsddb
import random
import time

DA_FILE_B = "/tmp/my_db/test_btree_db"
DA_FILE_H = "/tmp/my_db/test_hashtable_db"
DB_SIZE = 6

def main():
    print("\n\n*********PAIRS***********")
    pairs=[('111','aaa'),('222','bbb'),('333','ccc'),('444','ddd'),('555','eee'),('666','fff'),]
    print(pairs)
    print("\n\n        b=tree   h=hash")
    a=input("What type?")
    if a == "b":
        try:
            db = bsddb.btopen(DA_FILE_B, "w")
        except:
            print("DB doesn't exist, creating a new one")
            db = bsddb.btopen(DA_FILE_B, "c")
    elif a == "h":
        try:
            db = bsddb.btopen(DA_FILE_H, "w")
        except:
            print("DB doesn't exist, creating a new one")
            db = bsddb.btopen(DA_FILE_H, "c")
    else:
        print("Bad input")
        return
    
    for pair in pairs:
        db[pair[0].encode(encoding='UTF-8')] = pair[1].encode(encoding='UTF-8')

        
    key1=input("\n\nKey=")
    test = db.get(key1.encode(encoding='UTF-8'))
    print(test)
    
    
    db.close()
    
main()