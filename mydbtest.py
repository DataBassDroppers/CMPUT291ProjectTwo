import bsddb3 as bsddb
import random
import time
import sys
import os

DA_FILE_B = "/tmp/my_db/btree_db"
DA_FILE_H = "/tmp/my_db/hashtable_db"
DA_FILE_IB = "/tmp/my_db/indexfile_btree_db"
DA_FILE_IH = "/tmp/my_db/indexfile_hash_db"
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
    
        if ans == 1:
            data = getData() 
            db = makeDB(dbType,data)
            print("----Syncing to disk----")
        
            if type(db) == tuple:
                db[0].sync()
                db[1].sync()
            else:
                db.sync()
            print("Sync complete.\n")
        elif ans == 2:
            db=openDB(dbType)
            
            if type(db) == tuple:
                keySearch(db[1])
            else:
                keySearch(db)
        elif ans == 3:
            db=openDB(dbType)

            if type(db) == tuple:
                dataSearch(db[0])
            else:
                dataSearch(db)
        elif ans == 4:
            db=openDB(dbType)
            
            if type(db) == tuple:
                rangeSearch(db[0], "btree")
            else:
                rangeSearch(db, dbType)
        elif ans == 5:
            destroyDB(dbType)
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
        return null, True
    
    if tmp > 0 and tmp < 7:
        return tmp, False
    else:
        print("Enter an option within range.")
        return null, True

def makeDB(dbtype,data):
    if dbtype == "btree":
        db = makeBTree(data)
    elif dbtype == "hash":
        db = makeHashTable(data)
    elif dbtype == "indexfile":
        db = makeIndexFile(data)  
    print()
    print("Database created and populated of type: "+ dbtype)
    print()
    return db

def destroyDB(dbtype):
    if dbtype == "btree":
        os.remove("/tmp/my_db/btree_db")
    elif dbtype == "hash":
        os.remove("/tmp/my_db/hashtable_db")
    elif dbtype == "indexfile":
        os.remove("/tmp/my_db/indexfile_hash_db")
        os.remove("/tmp/my_db/indexfile_btree_db")


def getData():

    random.seed(SEED)

    keyValuePairs = []
    
    for index in range(DB_SIZE):
        krng = 64 + get_random()
        key = ""
        for i in range(krng):
            key += str(get_random_char())
        vrng = 64 + get_random()
        value = ""
        for i in range(vrng):
            value += str(get_random_char())

        key = key.encode(encoding='UTF-8')
        value = value.encode(encoding='UTF-8')
        
        keyValuePairs.append( (key, value) )

    return keyValuePairs

def makeBTree(data):
    try:
        db = bsddb.btopen(DA_FILE_B, "w")
    except:
        print("DB doesn't exist, creating a new one")
        db = bsddb.btopen(DA_FILE_B, "c")

    for pair in data:
        db[pair[0]] = pair[1]

    return db

def makeHashTable(data):
    try:
        db = bsddb.hashopen(DA_FILE_H, "w")
    except:
        print("DB doesn't exist, creating a new one")
        db = bsddb.hashopen(DA_FILE_H, "c")

    for pair in data:
        db[pair[0]] = pair[1]    

    return db


def makeIndexFile(data):
    try:
        dbBTree = bsddb.btopen(DA_FILE_IB, "w")
        dbHash = bsddb.hashopen(DA_FILE_IH, "w")
    except:
        print("DB doesn't exist, creating a new one")
        dbBTree = bsddb.btopen(DA_FILE_IB, "c")
        dbHash = bsddb.hashopen(DA_FILE_IH, "c")

    for pair in data:
        dbBTree[pair[0]] = pair[1]
        dbHash[pair[0]] = pair[1]

    return (dbBTree, dbHash)
    
    

#tests the databases and prints out results    
def test(database, parameters):

    tmp = 0
    for i in range(4):
        value = testKey(database, parameters[i][0])
        tmp += value
        print('Key search test ' + str(i) + ' time in ms: '+ \
                  str(value))
    print('Key search test average time in ms: ' + str(tmp/4))
    
    tmp = 0
    print('\n\n')
    for i in range(4):
        value = testReverse(database, parameters[i][1])
        tmp += value
        print('Reverse search test ' + str(i) + ' time: '+ \
                  str(value))
    print('Reverse search test average time in ms: ' +  str(tmp/4))  
        
    tmp = 0    
    print('\n\n')
    for i in range(4):
        value = testRange(database, parameters[i][0], parameters[(i + 1) % 4][0])
        tmp += value
        print('Range search test ' + str(i) + ' time: '+ \
                  str(value))
    print('Range search test average time in ms: ' + str(tmp/4))    
        
    
# answer should be the tuple of (key, value)
def writeAnswers(answer):
    file = open('answers', 'a')
    file.write(str(answer[0]) + '\n')
    file.write(str(answer[1]) + '\n')
    file.write('\n')           


def get_random():
    return random.randint(0, 63)
def get_random_char():
    return chr(97 + random.randint(0, 25))

def openDB(string):
    if string == "btree":
        db = bsddb.btopen(DA_FILE_B, "r")
    elif string == "hash":
        db = bsddb.hashopen(DA_FILE_H, "r")
    elif string == "indexfile":
        db = (bsddb.btopen(DA_FILE_B, "r"), bsddb.hashopen(DA_FILE_H, "r"))
    return db
    

def keySearch(database):
    key= input("Please enter key: ")
    key = key.encode(encoding='UTF-8')
    before = time.time() * 1000
    answer = database.get(key)
    after = time.time() * 1000
    print()
    if answer == None:
        print("Entries retrieved: 0") 
    else:
        print("Entries retrieved: 1")
        writeAnswers(answer)
    print("Total execution time in ms: " + str(after-before))
    
def dataSearch(database):
    value = input("Please enter a data value: ")
    value = value.encode(encoding='UTF-8')    
    before = time.time() * 1000
    matches = []
    last = database.last()
    current = database.first()
    
    while current != last: 
        if (current[1] == value):
            matches.append(current)
        current = database.next()
    after = time.time() * 1000
    
    print()
    print("Entries retrieved: " + str(len(matches)))
    print("Total execution time in ms: " + str(after-before))
    
    for each in matches:
        writeAnswers(each)
    
    return 1


def rangeSearch(database, dbType):

    lower = input("Please enter the start of the range: ")
    upper = input("Please enter the end of the range: ")

    lower = lower.encode(encoding='UTF-8')
    upper = upper.encode(encoding='UTF-8')

    if lower > upper:
        print("The start is lower than the end. Try again.")
        return

    if dbType == "btree":
        return rangeSearchBTree(database, lower, upper)
    elif dbType == "hash":
        return rangeSearchHash(database, lower, upper)

def rangeSearchBTree(database, lower, upper):
    before = time.time() * 1000

    values = []

    last = database.last()
    current = database.set_location(lower)


    while current[0] < upper and current != last:
        values.append(current)
        current = database.next()

    print("Values found within range: " + str(len(values)))
    print("Total execution time in ms: " + str(time.time()*1000 - before))
    return 1


def rangeSearchHash(database, lower, upper):

    before = time.time() * 1000

    values = []

    last = database.last()
    current = database.first()

    while current != last:
        if current[0] > lower and current[0] < upper:
            values.append(current)

        current = database.next()

    print("Values found within range: " + str(len(values)))
    print("Total execution time in ms: " + str(time.time()*1000 - before))
    return 1        

    
if __name__ == "__main__":
    main()

