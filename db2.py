from time import time,sleep
from threading import Thread,Lock
from ast import literal_eval
from queue import Queue
from os import path,makedirs,listdir
from concurrent.futures import ThreadPoolExecutor
from psutil import virtual_memory
from itertools import tee
import pickle

class Sample:
    def __init__(self, x, y, z, content,delete=False): # Only these parameters are Saved
        self.key = (x,y,z)        # Always have a key which is made from the parameters
        self.delete = False     # Always have a delete that is defaults to False
        self.lastUsed = 0       # Always have a lastUsed that is a number
        self.content = content  # Add any others you require

def create_objects(x,y,z):
    for i in range(x):
        for j in range(y):
            for k in range(z):
                yield Sample(i,j,k,"FFF")

class db:
    def __init__(self,fileName,object):
        startTime = time()
        self.running = True
        self.path = f"db/{fileName}"
        if not path.exists(self.path):
            makedirs(self.path)
            
        self.groupSize = 1000000 if type(object)==int else round(1000000**(1/len(object.key)))
        self.groups= [literal_eval(f) for f in listdir(self.path) if path.isfile(path.join(self.path, f))]
        self.datas = {}
        self.unloadTime = 30
        self.fileLock = Lock()
        self.executor = ThreadPoolExecutor()
        memory = virtual_memory()
        self.memory_free = memory.free 
        print('Free',self.memory_free)
        self.memory_percent = memory.percent
        self.memory_group = 95*self.groupSize
        
        self.executor.submit(self.run)
        print(f"Init Time:{time()-startTime}")
        
    def getGroup(self,key):
        if type(key) == int:
            return int(key/self.groupSize)
        # print(f"Key:{key}\t GroupKey:{tuple(floor(k/self.groupSize) for k in key)}")
        return tuple(int(k/self.groupSize) for k in key)
    def loadGroup(self,key):
        if key in self.datas:
            self.datas[key]=(self.datas[key][0],time())
            return
        p = self.path+'/'+str(key)
        if path.exists(p):
            with self.fileLock:
                print(f"LoadGroup:{key}")
                with open(p,"rb") as f:
                    self.datas[key] = [pickle.load(f),time()]
        else:
            print(f"EmptyLoadedGroup:{key}")
            self.datas[key] = [{},time()]
    def unloadGroup(self,key):
        p = self.path+'/'+str(key)
        with self.fileLock:
            print(f"UnloadGroup:{key}")
            with open(p,"wb") as f:
                pickle.dump(self.datas.pop(key)[0],f,protocol=pickle.HIGHEST_PROTOCOL)
        if key not in self.groups:
            self.groups.append(key)
    
    
    def readByKey(self,key):
        groupKey = self.getGroup(key)
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
            self.loadGroup(groupKey)
            result.extend([data for data in self.datas[groupKey][0].values() if data.key >= key1 and data.key<= key2])
            self.unloadGroup(groupKey)
        return result
    def readByField_Range(self,field,value1,value2):
        result=[]
        for groupKey in self.groups:
            self.loadGroup(groupKey)
            result.extend([data for data in self.datas[groupKey][0].values() if getattr(data,field) >= value1 and getattr(data,field)<= value2])
            self.unloadGroup(groupKey)
        return result
    
    
    def write(self,data_):
        groupsDone = []
        datas = list(tee(data_,3))
        print(datas)
        for data in datas.pop():
            groupKey = self.getGroup(data.key)
            if groupKey not in groupsDone:
                groupsDone.append(groupKey)
                datas.extend(list(tee(datas.pop(),2)))
                self.executor.submit(self.writeToGroup,datas.pop(),groupKey)
                # if self.memory_free >= self.memory_group*2:
                #     self.executor.submit(self.writeToGroup,datas2,groupKey)
                # else:
                #     self.writeToGroup(datas2,groupKey)
            
    def writeToGroup(self,datas,groupKey):
        self.loadGroup(groupKey)
        for data in datas:
            if self.getGroup(data.key) == groupKey:
                self.loadGroup(groupKey)
                self.datas[groupKey][0][data.key] = data
            else:
                del data
    
    def delete(self,key):
        groupKey = self.getGroup(key)
        self.loadGroup(groupKey)
        self.datas[groupKey][0].pop(key)
        
        
    def save(self):
        keys = list(self.datas.keys())
        for key in keys:
            # self.unloadGroup(key)
            self.executor.submit(self.unloadGroup,key)
    def stop(self):
        self.running=False
        self.save()
    def run(self):
        while self.running:
            sleep(0.001)
            memory = virtual_memory()
            self.memory_free = memory.free 
            self.memory_percent = memory.percent
            if self.memory_percent < 50:
                continue
            unloadTime = self.unloadTime * (90-self.memory_percent)/100
            if unloadTime <1: 
                unloadTime = 1
            for key,data in sorted(self.datas.items(), key=lambda item: item[1][1]):
                if time()-data[1] >= unloadTime:
                    self.unloadGroup(key)
                    break

def run():
    worldDb = db("world",Sample(0,0,0,"FFF"))
    objs = create_objects(100,200,200)
    worldDb.write(objs)
    # print(worldDb.readByKey((920,2)).key)
    # print(worldDb.readByField('key',(920,2)).key)
    # print([data.key for data in worldDb.readByKey_Range((0,0,0),(200,300,300))])
    # print([data.key for data in worldDb.readByField_Range('key',(123,2),(432,2))])
    sleep(30)
    print(f"Stop")
    startTime = time()
    worldDb.stop()
    print(f"Save Time: {time()-startTime}")
    
    
if __name__ == "__main__":
    startTime = time()
    run()
    print(f"Run Time: {time()-startTime}")