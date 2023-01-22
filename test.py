import pickle
from time import time

startTime=time()
data = [i for i in range(1000000)]
print(f"init Time: {time()-startTime}")
startTime=time()
with open('data.pickle', 'wb') as f:
    f.write(pickle.dumps(data))
print(f"init Time: {time()-startTime}")
with open('data.pickle', 'rb') as f:
    print(f"Data: {pickle.load(f)}")
