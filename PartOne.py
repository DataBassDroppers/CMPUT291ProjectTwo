import bsddb3 as bsddb
import random
import time

DA_FILE_B = "/tmp/my_db/btree_db"
DA_FILE_H = "/tmp/my_db/hashtable_db"
DB_SIZE = 1000
SEED = 10000000

def main():

    data = getData()
    
    bTree = makeBTree(data)
    hashTable = makeHashTable(data)

    parameters = getParameters(data)    
    print( type(bTree))
    print('Testing B Tree:\n')
    test(bTree, parameters)

    print('\n\n\n\n\n\nTesting Hash Table:\n')
    test(hashTable, parameters)

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
def test(database, parameters):

    for i in range(4):
        print('Key search test ' + str(i) + ' time in ms: '+ \
                  str(testKey(database, parameters[i][0])))
        
    print('\n\n')
    for i in range(4):
        print('Reverse search test ' + str(i) + ' time: '+ \
                  str(testReverse(database, parameters[i][1])))
    print('\n\n')
    for i in range(4):
        print('Range search test ' + str(i) + ' time: '+ \
                  str(testRange(database, parameters[i][0], parameters[(i + 1) % 4][0])))
        
def testKey(database, key):
    before = time.time() * 1000
    test = database[key]
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

