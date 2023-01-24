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

def create_objects(num_objects):
    for i in range(num_objects):
        yield Sample(i,2,"FFF")

class db:
    def __init__(self,fileName,object):
        startTime = time()
        self.path = f"db/{fileName}"
        if not path.exists(self.path):
            makedirs(self.path)
            
        self.groupSize = 10000 if type(object)==int else int(10000**(1/len(object.key)))
        self.groups= [literal_eval(f) for f in listdir(self.path) if path.isfile(path.join(self.path, f))]
        self.datas = {}
        
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
        else:
            self.datas[key] = {}
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
        data = self.datas[groupKey][key]
        self.unloadGroup(groupKey)
        return data
    def readByField(self,field,value):
        for group in self.groups:
            self.loadGroup(group)
            for data in self.datas[group]:
                if getattr(data,field) == value:
                    self.unloadGroup(group)
                    return data
    def readByKey_Range(self,key1,key2):
        result=[]
        groupKey1 = self.getGroup(key1)
        groupKey2 = self.getGroup(key2)
        if type(key1) == int:
            groupKeys = [group.key for group in self.groups if group.key >= groupKey1 and group.key<= groupKey2]
        else:
            groupKeys = [group.key for group in self.groups for i in range(len(key1)) 
                         if group.key[i] >= groupKey1[i] and group.key[i]<= groupKey2[i]]
        for groupKey in groupKeys:
            if groupKey not in self.datas:
                self.loadGroup(groupKey)
            if type(key1) == int:
                result.extend([data for data in self.datas[groupKey] if data.key >= key1 and data.key<= key2])
            else:
                result.extend([data for data in self.datas[groupKey] for i in range(len(key1)) 
                               if data.key[i] >= key1[i] and data.key[i]<= key2[i]])
            self.unloadGroup(groupKey)
        return result
    def readByField_Range(self,field,value1,value2):
        result=[]
        for groupKey in self.groups:
            if groupKey not in self.datas:
                self.loadGroup(groupKey)
            result.extend([data for data in self.datas[groupKey] if getattr(data,field) >= value1 and getattr(data,field)<= value2])
            self.unloadGroup(groupKey)
        return result
    
    
    def write(self,datas):
        for data in datas:
            groupKey = self.getGroup(data.key)
            if groupKey not in self.datas:
                self.loadGroup(groupKey)
            self.datas[groupKey][data.key] = data
            self.unloadGroup(groupKey)
    
    
    def delete(self,key):
        groupKey = self.getGroup(key)
        if groupKey not in self.datas:
            self.loadGroup(groupKey)
        self.datas[groupKey].pop(key)
        self.unloadGroup(groupKey)
        
def run():
    worldDb = db("world",Sample(0,0,"FFF"))
    objs = create_objects(1000)
    # worldDb.write(objs)
    print(worldDb.readByField('key',(420,2)).key)
    
    
if __name__ == "__main__":
    startTime = time()
    run()
    print(f"Run Time: {time()-startTime}")