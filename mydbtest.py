import bsddb3 as bsddb
import random
import time
import sys

DA_FILE_B = "/tmp/my_db/btree_db"
DA_FILE_H = "/tmp/my_db/hashtable_db"
DB_SIZE = 10000
SEED = 10000000


#TODO: Implement makeIndexFile(data)
#      Implement keySearch()
#      Implement dataSearch()
#      Implement rangeSearch()


def main():
    
    if len(sys.argv) != 2:
        print("Invalid number of arguments, expected 1 but received " + str(len(sys.argv) - 1))
        return 0
    tmp = ["btree", "hash", "indexfile"]
    if sys.argv[1] not in tmp:
        print("Invalid argument, should be 'btree', 'hash', or 'indexfile'")
        return 0
    
    data = getData()  

    cont = True
    
    while quit:
    
        go = False
        while go:
            ans, go = menu()    
    
        if ans == 1:
            db = makeDB(sys.argv[1])
        elif ans == 2:
            keySearch()
        elif ans == 3:
            dataSearch()
        elif ans == 4:
            rangeSearch()
        elif ans == 5:
            db.close()
        elif ans == 6:
            cont = True
    



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
        return null, False
    
    if tmp > 0 and tmp < 7:
        return tmp, True
    else:
        print("Enter an option within range.")
        return null, False

def makeDB(dbtype):
    if dbtype == "btree":
        db = makeBTree(data)
    elif dbtype == "hash":
        db = makeHashTable(data)
    elif dbtype == "indexfile":
        db = makeIndexFile(data)    
    return db

def getData():

    random.seed()

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
        
def testKey(database, key):
    before = time.time() * 1000
    test = database.get(key)
    return time.time() * 1000 - before

def testReverse(database, value):
    before = time.time() * 1000

    matches = []

    last = database.last()
    current = database.first()
    
    
    while current != last: 
        if (current[1] == value):
            matches.append(current)
        current = database.next()

    return time.time() * 1000 - before

def testRange(database, key1, key2):

    lower = min(key1, key2)
    upper = min(key1, key2)

    before = time.time() * 1000

    values = []

    current = database.set_location(lower)
    
    while current[0] != upper:
        values.append(current)
        current = database.next()

    return time.time() * 1000 - before
    

#randomly generate some parameters for testing
def getParameters(data):
    
    randomInts = []
    randomInts.append(random.randint(0, DB_SIZE))
    randomInts.append(random.randint(0, DB_SIZE))
    randomInts.append(random.randint(0, DB_SIZE))
    randomInts.append(random.randint(0, DB_SIZE))

    params = []
    params.append(data[randomInts[0]])
    params.append(data[randomInts[1]])
    params.append(data[randomInts[2]])
    params.append(data[randomInts[3]])
                            
    return params



def get_random():
    return random.randint(0, 63)
def get_random_char():
    return chr(97 + random.randint(0, 25))

if __name__ == "__main__":
    main()

