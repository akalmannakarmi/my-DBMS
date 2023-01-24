from time import time,sleep
from threading import Thread
from ast import literal_eval
from os import path,makedirs,listdir
from concurrent.futures import ThreadPoolExecutor
from psutil import virtual_memory
import pickle

class Sample:
    def __init__(self, x, y, content,delete=False): # Only these parameters are Saved
        self.key = (x,y)        # Always have a key which is made from the parameters
        self.delete = False     # Always have a delete that is defaults to False
        self.lastUsed = 0       # Always have a lastUsed that is a number
        self.content = content  # Add any others you require

def create_objects(x,y):
    for i in range(x):
        for j in range(y):
            yield Sample(i,j,"FFF")

class db:
    def __init__(self,fileName,object):
        startTime = time()
        self.running = True
        self.path = f"db/{fileName}"
        if not path.exists(self.path):
            makedirs(self.path)
            
        self.groupSize = 10000 if type(object)==int else int(10000**(1/len(object.key)))
        self.groups= [literal_eval(f) for f in listdir(self.path) if path.isfile(path.join(self.path, f))]
        self.datas = {}
        self.unloadTime = 60
        self.memory = virtual_memory()
        self.executor = ThreadPoolExecutor()
        
        self.executor.submit(self.run)
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
                self.datas[key] = (pickle.load(f),time())
        else:
            self.datas[key] = ({},time())
    def unloadGroup(self,key):
        p = self.path+'/'+str(key)
        with open(p,"wb") as f:
            pickle.dump(self.datas.pop(key)[0],f)
        if key not in self.groups:
            self.groups.append(key)
    
    
    def readByKey(self,key):
        groupKey = self.getGroup(key)
        if groupKey not in self.datas:
            self.loadGroup(groupKey)
        data = self.datas[groupKey][0][key]
        self.unloadGroup(groupKey)
        return data
    def readByField(self,field,value):
        for group in self.groups:
            self.loadGroup(group)
            for data in self.datas[group][0].values():
                if getattr(data,field) == value:
                    self.unloadGroup(group)
                    return data
    def readByKey_Range(self,key1,key2):
        result=[]
        groupKey1 = self.getGroup(key1)
        groupKey2 = self.getGroup(key2)
        groupKeys = [key for key in self.groups if key >= groupKey1 and key<= groupKey2]
        for groupKey in groupKeys:
            if groupKey not in self.datas:
                self.loadGroup(groupKey)
            result.extend([data for data in self.datas[groupKey][0].values() if data.key >= key1 and data.key<= key2])
            self.unloadGroup(groupKey)
        return result
    def readByField_Range(self,field,value1,value2):
        result=[]
        for groupKey in self.groups:
            if groupKey not in self.datas:
                self.loadGroup(groupKey)
            result.extend([data for data in self.datas[groupKey][0].values() if getattr(data,field) >= value1 and getattr(data,field)<= value2])
            self.unloadGroup(groupKey)
        return result
    
    
    def write(self,datas):
        for data in datas:
            groupKey = self.getGroup(data.key)
            if groupKey not in self.datas:
                self.loadGroup(groupKey)
            self.datas[groupKey][0][data.key] = data
    
    def delete(self,key):
        groupKey = self.getGroup(key)
        if groupKey not in self.datas:
            self.loadGroup(groupKey)
        self.datas[groupKey][0].pop(key)
        
        
    def save(self):
        keys = list(self.datas.keys())
        for key in keys:
            self.unloadGroup(key)
    def stop(self):
        self.running=False
        self.save()
    def run(self):
        while self.running:
            sleep(0.0001)
            memory_percent = self.memory.used / self.memory.total * 100
            if memory_percent < 60:
                continue
            unloadTime = self.unloadTime * (90-memory_percent)/100
            if unloadTime <=0: 
                unloadTime = 0.01 
            for key in list(self.datas.keys()):
                if time()-self.datas[key][1] >= unloadTime:
                    self.unloadGroup(key)

def run():
    worldDb = db("world",Sample(0,0,"FFF"))
    objs = create_objects(1000,1000)
    worldDb.write(objs)
    # print(worldDb.readByKey((920,2)).key)
    # print(worldDb.readByField('key',(920,2)).key)
    # print([data.key for data in worldDb.readByKey_Range((123,2),(432,2))])
    # print([data.key for data in worldDb.readByField_Range('key',(123,2),(432,2))])
    sleep(15)
    print(f"Stop")
    startTime = time()
    worldDb.stop()
    print(f"Save Time: {time()-startTime}")
    
    
if __name__ == "__main__":
    startTime = time()
    run()
    print(f"Run Time: {time()-startTime}")