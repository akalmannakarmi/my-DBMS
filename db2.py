from time import time,sleep
from threading import Thread
from ast import literal_eval
from os import path,makedirs,listdir
from concurrent.futures import ThreadPoolExecutor
import pickle

class Sample:
    def __init__(self, x, y, content,delete=False): # Only these parameters are Saved
        self.key = (x,y)        # Always have a key which is made from the parameters
        self.delete = False     # Always have a delete that is defaults to False
        self.lastUsed = 0       # Always have a lastUsed that is a number
        self.content = content  # Add any others you require

class db:
    def __init__(self,fileName,object):
        startTime = time()
        self.path = f"db/{fileName}"
        self.groupSize = 10000 if type(object)==int else int(10000**(1/len(object.key)))
        self.groups= [literal_eval(f[: -4]) for f in listdir(self.path) if path.isfile(path.join(self.path, f))]
        self.datas = {}
        if not path.exists(self.path):
            makedirs(self.path)
        print(f"Init Time:{time()-startTime}")
        
    def getGroup(self,key):
        if type(key) == int:
            return int(key/self.groupSize)
        return tuple(int(k/self.groupSize) for k in key)
    def loadGroup(self,key):
        if key in self.datas:
            return
        p = self.path+'/'+str(key)
        if path.exists(p):
            with open(p,"rb") as f:
                self.datas[key] = pickle.load(f)
    def unloadGroup(self,key):
        p = self.path+'/'+str(key)
        with open(p,"wb") as f:
            pickle.dump(self.datas.pop(key),f)
        if key not in self.groups:
            self.groups.append(key)
    
    
    def readByKey(self,key):
        groupKey = self.getGroup(key)
        if groupKey not in self.datas:
            self.loadGroup(groupKey)
        return self.datas[groupKey][key]
    def readByField(self,field,value):
        pass
def run():
    worldDb = db("world",Sample(0,0,"FFF"))
    worldDb.readByField(1,1)
    
    
if __name__ == "__main__":
    startTime = time()
    run()
    print(f"Run Time: {time()-startTime}")