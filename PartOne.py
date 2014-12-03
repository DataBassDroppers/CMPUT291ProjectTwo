import bsddb3 as bsddb
import random
import time
import DatabaseFunctions

DA_FILE_B = "/tmp/my_db/btree_db"
DA_FILE_H = "/tmp/my_db/hashtable_db"
DA_FILE_IB = "/tmp/my_db/indexfile_btree_db"
DA_FILE_IRB = "/tmp/my_db/indexfile_rbtree_db"
DB_SIZE = 100000
SEED = 10000000

def main():

    data = getData()
    
    bTree = makeBTree(data)
    hashTable = makeHashTable(data)
    indexFile = DatabaseFunctions.makeIndexFile(data)
    

    parameters = getParameters(data)    

    print('Testing B Tree:\n')
    test(bTree, parameters, 'btree')

    print('\n\n\n\n\n\nTesting Hash Table:\n')
    test(hashTable, parameters, 'hash')
    
    print('\n\n\n\n\n\nTesting Index File:\n')    
    test(indexFile, parameters, 'indexfile')

    bTree.close()
    hashTable.close()

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
def test(database, parameters, dbType):

    tmp = 0

    if dbType == 'indexfile':
        database[0].first()
    else:
        database.first()
            
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

    lowerBounds = ['a','d','f','f']
    upperBounds = ['c','f','g','fi']
    for i in range(4):
        key1 = lowerBounds[i].encode(encoding = 'UTF-8')
        key2 = upperBounds[i].encode(encoding = 'UTF-8')

        if dbType == 'indexfile':
            value = DatabaseFunctions.rangeSearchBTree(database[2], key1, key2, True)
        elif dbType == 'btree':
            value = DatabaseFunctions.rangeSearchBTree(database, key1, key2, True)
        elif dbType == 'hash':
            value = DatabaseFunctions.rangeSearchHash(database, key1, key2, True)
        else:
            print('ERROR')
        tmp += value
        
        print('Range search test ' + str(i) + ' time: '+ \
                  str(value))
    print('Range search test average time in ms: ' + str(tmp/4))    
        
def testKey(database, key):
    if type(database) == tuple:
        database = database[0]
    
    before = time.time() * 1000
    test = database.get(key)
    return time.time() * 1000 - before

def testReverse(database, value):
    if type(database) == tuple:
        database = database[1]
        return testIndexReverse(database,value)        
        
    before = time.time() * 1000

    matches = []

    last = database.last()
    current = database.first()
    
    
    while current != last: 
        if (current[1] == value):
            matches.append(current)
        current = database.next()

    return time.time() * 1000 - before

def testIndexReverse(database, value):
    
    before = time.time() * 1000
    keyString = database.get(value)
    after = time.time() * 1000
    
    keyList = keyString.split()    

    
    matches = []
    for key in keyList:
        matches.append((key,value))
    
    return after - before    

    

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

