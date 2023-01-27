import pickle
from time import time
from itertools import tee

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

# from db import db,Sample

# def create_objects(num_objects):
#     for i in range(num_objects):
#         yield Sample(i,2,"FFF")
        
# startTime=time()

# testdb = db("test")
# testdb.F_write(create_objects(10000000))
# # print(testdb.readByField('key', (69420,1)).key)

# testdb.stop()

# import psutil

# # Get system memory information
# memory = psutil.virtual_memory()

# startTime=time()
# # Calculate the percentage of memory used
# memory_percent = memory.used / memory.total * 100

# # Print the percentage of memory used
# print(f'Memory used: {memory_percent:.2f}%')
# print(f"init Time: {time()-startTime}")


first_coord = (-1, 2, 3)
second_coord = (0, 0, 0)
third_coord = (2, 4, 6)

for a, b, c in zip(first_coord, second_coord, third_coord):
    if b <= a <= c:
        continue
    else:
        print(first_coord, "is not between", second_coord, "and", third_coord)
        break
else:
    print(first_coord, "is between", second_coord, "and", third_coord)
