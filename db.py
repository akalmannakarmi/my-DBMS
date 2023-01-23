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
    def stop(self):
        print("Stop")
        startTime = time()
        self.running=False
        
        save = self.writeAll if self.update else self.saveIndex
        save()
        self.file.close()
        self.indexFile.close()
        print(f"{self.fileName} closed: {time()-startTime}")
        
    def __init__(self,fileName):
        startTime = time()
        self.running=True
        self.update=False
        self.file_writing=False
        self.R_file_reading=False
        self.fileName = "db/"+fileName
        self.content={}
        self.index={}
        self.queWrite = Queue()
        self.queAlter = Queue()
        self.executor = ThreadPoolExecutor()
        
        if not path.exists("db"):
            makedirs("db")
        # Loading db data
        self.loadMetaData()
        
        # Opening most required files
        p = f"{self.fileName}{self.ver}.bin"
        self.file = open(p, 'r+b') if path.exists(p) else open(p, 'w+b')
        self.R_file = open(p, 'rb')
        p = f"{self.fileName}{self.iVer}_index.bin"
        self.indexFile = open(p, 'r+b') if path.exists(p) else open(p, 'w+b')
        self.file.
        # Load data
        self.loadIndex()
        
        # Run bg worker
        self.executor.submit(self.run)
        print(f"{self.fileName}{self.ver} opened: {time()-startTime}")
            
            
    def loadIndex(self):
        try:
            self.indexFile.seek(0)
            self.index = pickle.load(self.indexFile)
        except EOFError:
            pass
    def saveIndex(self):
        iVer = 0 if self.iVer else 1
        indexFile=open(f"{self.fileName}{iVer}_index.bin", "w+b")
        pickle.dump(self.index, indexFile, protocol=pickle.HIGHEST_PROTOCOL)
        self.indexFile.close()
        self.indexFile = indexFile
        self.iVer = iVer
        self.saveMetaData()
    
    def loadMetaData(self):   
        if path.exists(f"{self.fileName}_metaData.bin"):
            with open(f"{self.fileName}_metaData.bin", "rb") as metaData_file:
                self.ver = pickle.load(metaData_file)
                self.iVer = pickle.load(metaData_file)
        else:
            self.ver = 0
            self.iVer = 0
    def saveMetaData(self):   
        with open(f"{self.fileName}_metaData.bin", "wb") as metaData_file:
            pickle.dump(self.ver,metaData_file)
            pickle.dump(self.iVer,metaData_file)
    
    
    def write_Now(self,datas):
        self.M_write(datas)
        self.F_write(datas)
    def write(self,datas):
        self.M_write(datas)
        self.queWrite.put(datas)
    def M_write(self,datas):
        datas=[datas] if type(datas) != list else datas
        for data in datas:
            if data.key in self.index.keys():
                raise ValueError(f"An item of the key already Exists key:{data.key}")
            self.content[data.key] = data
    def F_write(self,datas):
        while self.file_writing:
            sleep(0.0001)
            
        self.file_writing= True
        self.file.seek(0, 2)
        for data in datas:
            if data.key in self.index.keys():
                print(f"An item of the key already Exists key:{data.key}")
                continue
            self.index[data.key]=self.file.tell()
            pickle.dump(data, self.file, protocol=pickle.HIGHEST_PROTOCOL)
            
        self.file_writing=False
                
    def writeAll(self):
        print("Write All")
        while self.file_writing:
            sleep(.0001)
            
        self.index= {}
        ver = 0 if self.ver else 1
        file = open(f"{self.fileName}{ver}.bin", "w+b")
        for key,data in self.content.items():
            if data.delete:
                continue
            self.index[key]=file.tell()
            pickle.dump(data, file, protocol=pickle.HIGHEST_PROTOCOL)
            
        self.file.seek(0)
        while True:
            try:
                data = pickle.load(self.file)
            except EOFError:
                break
            if data.delete or data.key in self.index.keys():
                continue
            self.index[data.key]=file.tell()
            pickle.dump(data, file, protocol=pickle.HIGHEST_PROTOCOL)
            
        self.file.close()
        self.file = file
        self.R_file = open(f"{self.fileName}{ver}.bin", "rb")
        self.ver = ver
        self.saveIndex()
        print("WriteAll Complete")
      
            
    def readByIndex(self,index):
        if index not in self.index.keys():
            return
        if index in self.content.keys():
            return self.content[index]
        else:
            while self.R_file_reading:
                sleep(0.0001)
            self.R_file_reading = True
            self.R_file.seek(self.index[index])
            data = pickle.load(self.R_file)   
            self.R_file_reading = False
            return data
    def readByField(self,field,value):
        for data in self.content.values():
            if getattr(data,field) == value:
                return data
        
        offset = 0
        while True:
            try:
                while self.R_file_reading:
                    sleep(0.0001)
                    
                self.R_file_reading=True
                self.R_file.seek(offset)
                data = pickle.load(self.R_file)
                offset = self.R_file.tell()
                self.R_file_reading=False
                
                if getattr(data,field) == value:
                    return data
            except EOFError:
                break
                
                
    def delete_Now(self,index):
        self.M_delete(index)
        self.F_delete(index)
    def delete(self,index):
        self.M_delete(index)
        self.queAlter.put((index,'delete',False))
    def m_delete(self,index):
        if index in self.content:
            return self.content.pop(index)
    def F_delete(self,index):
        self.update=True
        self.F_alter(self,index,'delete',True)
            
                
    def alter_Now(self,index,field,value):
        self.M_alter(index,field,value)
        self.F_alter(index,field,value)
    def alter(self,index,field,value):
        self.M_alter(index,field,value)
        self.queAlter.put((index,field,value))
    def M_alter(self,index,field,value):
        setattr(self.content[index],field,value)
    def F_alter(self,index,field,value):
        while self.R_file_reading and self.file_writing:
            sleep(0.0001)
        if self.file_writing:
            self.R_file_reading=True
            self.R_file.seek(self.index[index])
            data = pickle.load(self.R_file)
            self.R_file_reading=False
        else:
            self.file_writing=True
            self.file.seek(self.index[index])
            data = pickle.load(self.file)
            self.file_writing=False
            
        while self.file_writing:
            sleep(0.0001)

        self.file_writing=True
        self.file.seek(self.index[index])
        setattr(data,field,value)
        pickle.dump(data,self.file)
        self.file_writing=False
    
    def keyAlter(self,Oindex,Nindex):
        self.alter(Oindex,'key',Nindex)
        self.index[Nindex] = self.index.pop(Oindex)
    def keyAlter_Now(self,Oindex,Nindex):
        self.alter_Now(Oindex,'key',Nindex)
        self.index[Nindex] = self.index.pop(Oindex)
    
        
    def run(self):
        while self.running:
            if self.queAlter.empty() and self.queWrite.empty():
                sleep(.0001)
                continue
            if not self.queWrite.empty():
                data = self.queWrite.get()
                try:
                    iter(data)
                except TypeError:
                    data = [data]
                self.F_write(data)
            if not self.queAlter.empty():
                self.F_alter(self.queAlter.get())
    
def run():
    worldDb = db("world")

    # startTime = time()
    # makeCoords(worldDb,0,0,10000,10000,"FFFF")
    # print(f"Assign Time : {(time()-startTime)}ms")

    # startTime = time()
    # worldDb.M_write(Sample(-42,2,"FFF2"))
    # print(f"Write Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # worldDb.writeMany(coords)
    # print(f"WriteMany Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # worldDb.writeAll()
    # print(f"WriteAll Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # worldDb.delete((-2,3))
    # print(f"Delete Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # data= worldDb.readIndex((-2,3))
    # print(f"Read Time : {(time()-startTime)}ms")
    
    # print(f"Data: {data.key} , {data.delete}")
    
    # startTime = time()
    # worldDb.writeAll()
    # print(f"WriteAll Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # data= worldDb.readIndex((2,40000))
    # print(f"Read Time : {(time()-startTime)}ms")
    
    # print(f"Data: {data.key} , {data.content}")
    
    # startTime = time()
    # worldDb.alter((2,40000),"EEEE")
    # print(f"Alter Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # data= worldDb.readByIndex((-42,3))
    # print(f"Read Time : {(time()-startTime)}ms")
    
    # print(f"Data: {data.key} , {data.content}")
    worldDb.printIndex()
    worldDb.stop()
    
    
    
if __name__ == "__main__":
    startTime = time()
    run()
    print(f"Run Time: {time()-startTime}")