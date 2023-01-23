from time import time,sleep
from queue import Queue
from threading import Thread
from os import path,makedirs
from concurrent.futures import ThreadPoolExecutor
import pickle

# todo


class Sample:
    def __init__(self, x, y, content,delete=False): # Only these parameters are Saved
        self.key = (x,y)        # Always have a key which is made from the parameters
        self.delete = False     # Always have a delete that is defaults to False
        self.lastUsed = 0       # Always have a lastUsed that is a number
        self.content = content  # Add any others you require

class db:
    def __init__(self,fileName,object):
        self.path = f"db/{fileName}/"
        self.groupSize = 10000 if type(object)==int else int(10000**(1/len(object.key)))
        self.datas = {}
        if not path.exists(self.path):
            makedirs(self.path)
        
    def getGroup(self,key):
        if type(key) == int:
            return int(key/self.groupSize)
        return tuple(int(k/self.groupSize) for k in key)
    
    def loadGroup(self,key):
        p = self.path+str(key)
        if path.exists(p):
            with open(p,"rb") as f:
                self.datas[key] = pickle.load(f)
    
    def saveGroup(self,key):
        p = self.path+str(key)
        if path.exists(p):
            with open(p,"wb") as f:
                pickle.dump(self.datas[key],f)
            
        
    
def run():
    worldDb = db("world",Sample(0,0,"FFF"))
    
    
if __name__ == "__main__":
    startTime = time()
    run()
    print(f"Run Time: {time()-startTime}")