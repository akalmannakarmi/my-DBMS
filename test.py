import pickle
from time import time

# data = [i for i in range(100000)]
# data_ = []

# startTime=time()
# with open('data.pickle', 'wb') as f:
#     pickle.dump(data,f)
# print(f"init Time: {time()-startTime}")

# startTime=time()
# with open('data.pickle', 'rb') as f:
#     data_=pickle.load(f)
# print(f"load Time: {time()-startTime}")

from db import db,Sample

def create_objects(num_objects):
    for i in range(num_objects):
        yield Sample(i,2,"FFF")
        
startTime=time()

testdb = db("test")
testdb.F_write(create_objects(10000000))
# print(testdb.readByField('key', (69420,1)).key)
print(f"work Time: {time()-startTime}")

testdb.stop()
print(f"init Time: {time()-startTime}")
