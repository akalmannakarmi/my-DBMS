from time import time,sleep
from os import path,makedirs,listdir
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor
from ast import literal_eval
from psutil import virtual_memory
from queue import Queue
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
    def __init__(self,name,object):
        startTime = time()
        self.running = True
        self.path = f"db/{name}"
        if not path.exists(self.path):
            makedirs(self.path)
        # Loading db data
        self.loadMetaData()
            
        # Opening most required files
        p = f"{self.path}/{name}{self.ver}.bin"
        self.file = open(p, 'r+b') if path.exists(p) else open(p, 'w+b')
        self.R_file = open(p, 'rb')
            
        self.groupSize = 1000000 if type(object)==int else round(1000000**(1/len(object.key)))
        self.groups= [literal_eval(f[: -4]) for f in listdir(self.path) if f.endswith('.inx') and path.isfile(path.join(self.path, f))]
        self.datas = {}
        self.indexes = {}
        self.unloadTime = 30
        
        self.fileLock = Lock()
        self.executor = ThreadPoolExecutor()
        
        memory = virtual_memory()
        self.memory_available = memory.available 
        self.memory_percent = memory.percent
        self.memory_group = 95*self.groupSize
        
        self.executor.submit(self.run)
        print(f"Init Time:{time()-startTime}")
        
        
    def loadMetaData(self):   
        if path.exists(f"{self.path}/metaData.bin"):
            with open(f"{self.path}/metaData.bin", "rb") as metaData_file:
                self.ver = pickle.load(metaData_file)
                self.iVer = pickle.load(metaData_file)
        else:
            self.ver = 0
            self.iVer = 0
    def saveMetaData(self):   
        with open(f"{self.path}/metaData.bin", "wb") as metaData_file:
            pickle.dump(self.ver,metaData_file)
            pickle.dump(self.iVer,metaData_file)
        
        
    def getGroup(self,key):
        if type(key) == int:
            return int(key/self.groupSize)
        return tuple(int(k/self.groupSize) for k in key)
    def loadGroup(self,key):
        if key in self.indexes:
            try:
                self.indexes[key][1]=time()
                return
            except KeyError:
                pass
        p = f"{self.path}/{key}.inx"
        if path.exists(p):
            with self.fileLock:
                print(f"LoadGroup:{key}")
                with open(p,"rb") as f:
                    self.indexes[key] = [pickle.load(f),time()]
        else:
            print(f"EmptyLoadedGroup:{key}")
            self.indexes[key] = [{}, time()]
    def unloadGroup(self,key):
        p = f"{self.path}/{key}.inx"
        with self.fileLock:
            print(f"UnloadGroup:{key}")
            with open(p,"wb") as f:
                pickle.dump(self.indexes.pop(key)[0],f,protocol=pickle.HIGHEST_PROTOCOL)
        if key not in self.groups:
            self.groups.append(key)
    def saveGroup(self,key):
        p = f"{self.path}/{key}.inx"
        with self.fileLock:
            print(f"UnloadGroup:{key}")
            with open(p,"wb") as f:
                pickle.dump(self.indexes[key][0],f,protocol=pickle.HIGHEST_PROTOCOL)
        if key not in self.groups:
            self.groups.append(key)
       
    
    def write_Now(self,datas):
        datas1,datas2= tee(datas)
        self.M_write(datas1)
        self.F_write(datas2)
    def write(self,datas):
        datas1,datas2= tee(datas)
        self.executor.submit(self.F_write,datas2)
        self.M_write(datas1)
    def M_write(self,datas):
        for data in datas:
            if data.key in self.datas:
                continue
            self.datas[data.key]=[data,time()]
    def F_write(self,datas):
        for data in datas:
            group = self.getGroup(data.key)
            self.loadGroup(group)
            if data.key in self.indexes[group][0]:
                continue
            with self.fileLock:
                offset=self.file.tell()
                pickle.dump(data, self.file, protocol=pickle.HIGHEST_PROTOCOL)
            self.loadGroup(group)
            self.indexes[group][0][data.key] = offset 
        
    def alterKey(self,oKey,nKey):
        oGroup = self.getGroup(oKey)
        nGroup = self.getGroup(nKey)
        self.loadGroup(oGroup)
        self.loadGroup(nGroup)
        self.groups[nGroup][0][nKey]=self.groups[oGroup][0].pop(oKey)
        self.saveGroup(oGroup)
        self.saveGroup(nGroup)
    
    def delete_Now(self,index):
        self.M_delete(index)
        self.saveGroup(self.getGroup(index))
    def delete(self,index):
        self.executor.shutdown(self.saveGroup,self.getGroup(index))
        self.M_delete(index)
    def m_delete(self,index):
        if index in self.datas:
            return self.datas.pop(index)
        
    def save(self):
        keys = list(self.indexes.keys())
        for key in keys:
            # self.unloadGroup(key)
            self.executor.submit(self.unloadGroup,key)
    def stop(self):
        self.running=False
        self.save()
        self.saveMetaData()
    def run(self):
        while self.running:
            sleep(0.001)
            memory = virtual_memory()
            self.memory_available = memory.available 
            self.memory_percent = memory.percent
            if self.memory_percent < 50:
                continue
            unloadTime = self.unloadTime * (90-self.memory_percent)/100
            if unloadTime <1: 
                unloadTime = 1
            for key,data in sorted(self.indexes.items(), key=lambda item: item[1][1]):
                if time()-data[1] >= unloadTime:
                    self.unloadGroup(key)
                    break
            for key,data in sorted(self.datas.items(), key=lambda item: item[1][1]):
                if time()-data[1] >= unloadTime:
                    del self.datas[key]
                
                
def run():
    worldDb = db("world",Sample(0,0,0,"FFF"))
    objs = create_objects(10000,100,100)
    worldDb.F_write(objs)
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