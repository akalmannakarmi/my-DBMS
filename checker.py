from time import time
from os import listdir,path
from ast import literal_eval
import pickle

class Sample:
    def __init__(self, x, y, z, content,delete=False): # Only these parameters are Saved
        self.key = (x,y,z)        # Always have a key which is made from the parameters
        self.delete = False     # Always have a delete that is defaults to False
        self.lastUsed = 0       # Always have a lastUsed that is a number
        self.content = content  # Add any others you require

datas={}
groupSize = 100
p = 'db/world'
def load(key):
    startTime = time()
    with open(p+'/'+str(key),'rb') as f:
        datas[key] = pickle.load(f)
    print(f"Load Time:{time()-startTime}")
def unload(key):
        del datas[key]
        
def check(key):
    load(key)
    print(f"Group : {key}")
    for i in range(key[0]*groupSize,key[0]*(groupSize+1)):
        for j in range(key[1]*groupSize,key[1]*(groupSize+1)):
            for k in range(key[2]*groupSize,key[2]*(groupSize+1)):
                if (i,j,k) not in datas[key]:
                    print(f"Key Missing:{(i,j,k)}")
                    raise 'break'
    unload(key)
                

def checkAll():
    groups= [literal_eval(f) for f in listdir(p) if path.isfile(path.join(p, f))]
    for  group in groups:
        check(group)

checkAll()
        