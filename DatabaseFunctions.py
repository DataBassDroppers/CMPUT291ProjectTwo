import bsddb3 as bsddb
import random
import time
import sys
import os

DA_FILE_B = "/tmp/rtwong_db/btree_db"
DA_FILE_H = "/tmp/rtwong_db/hashtable_db"

DA_FILE_IB = "/tmp/rtwong_db/indexfile_btree_db"
DA_FILE_IRB = "/tmp/rtwong_db/indexfile_reverse_btree_db"


DB_SIZE = 100
SEED = 10000000




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
    try:
        if dbtype == "btree":
            os.remove("/tmp/rtwong_db/btree_db")
        elif dbtype == "hash":
            os.remove("/tmp/rtwong_db/hashtable_db")
        elif dbtype == "indexfile":
            os.remove(DA_FILE_IB)
            os.remove(DA_FILE_IRB)
    except OSError or bsddb.db.DBNoSuchFileError:
        pass




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
        dbRBTree = bsddb.btopen(DA_FILE_IRB, "w")
        
    except:
        print("DB doesn't exist, creating a new one")
        dbBTree = bsddb.btopen(DA_FILE_IB, "c")
        dbRBTree = bsddb.btopen(DA_FILE_IRB, "c")

    for pair in data:
        dbBTree[pair[0]] = pair[1]
        
        try:
            exists = dbRBTree.get(pair[1])
            exists = exists + " " + pair[0]
        except:
            dbRBTree[pair[1]] = pair[0]

    return (dbBTree, dbRBTree)
    
    
    
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
        db = (bsddb.btopen(DA_FILE_IB, "r"), bsddb.btopen(DA_FILE_IRB, "r"))
    return db
    

def keySearch(database, key, suppressMessages = False):
    
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
    return after - before
    
def dataSearch(database, dbType, value, suppressMessages = False):

    
    if dbType == "btree" or dbType == "hash":
        return dataSearchBH(database, value)
    elif dbType == "indexfile":
        return dataSearchIF(database, value)
    
    
def dataSearchBH(database, value, suppressMessages = False):    
    
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
    
    return after - before

def dataSearchIF(database, value):    

    before = time.time() * 1000
    keyString = database.get(value)
    after = time.time() * 1000

    if keyString == None:
        keyList= []
    else:
        keyList = keyString.split()    
    
    print()
    print("Entries retrieved: " + str(len(keyList)))
    print("Total execution time in ms: " + str(after-before))
    
    for each in keyList:
        writeAnswers(each)
    
    return after - before


def rangeSearchBTree(database, lower, upper, suppressMessages = False):
    before = time.time() * 1000
    values = []

    last = database.last()
    current = database.set_location(lower)

    while current[0] < upper:
        values.append(current)

        try:
            current = database.next()
        except bsddb.db.DBNotFoundError:
            break;

    after = time.time() * 1000

    for each in values:
        writeAnswers(each)
        
    print("Values found within range: " + str(len(values)))
    print("Total execution time in ms: " + str(after - before))
    return after - before


def rangeSearchHash(database, lower, upper, suppressMessages = False):

    before = time.time() * 1000

    values = []

    last = database.last()
    current = database.first()

    while True:
        if current[0] > lower and current[0] < upper:
            values.append(current)

        try:
            current = database.next()
        except bsddb.db.DBNotFoundError:
            break;

    after = time.time() * 1000

    for each in values:
        writeAnswers(each)

    print("Values found within range: " + str(len(values)))
    print("Total execution time in ms: " + str(after - before))
    return after - before


