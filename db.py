from time import time,sleep
from queue import Queue
from threading import Thread
from os import path,makedirs
from concurrent.futures import ThreadPoolExecutor
import pickle

executor = ThreadPoolExecutor()

def makeCoords(db,x1,y1,x2,y2,value):
    if x2-x1>100 or y2-y1>100:
        x=x1
        while x2>x:
            dx = 100 if x2-x>100 else x2-x
            y=y1
            while y2>y:
                dy = 100 if y2-y>100 else y2-y
                makeCoords(db,x,y,x+dx,y+dy,value)
                sleep(.01)
                y+= dy
            x+= dx
    else:
        for i in range(x1,x2):
            for j in range(y1,y2):
                # db.M_write(Sample(i,j,f"{(i,j)}:{value}"))
                db.write(Sample(i,j,f"{(i,j)}:{value}"))
        
class Sample:
    def __init__(self, x, y, content,delete=False): # Only these parameters are Saved
        self.key = (x,y)        # Always have a key which is made from the parameters
        self.delete = False     # Always have a delete that is defaults to False
        self.lastUsed = 0       # Always have a lastUsed that is a number
        self.content = content  # Add any others you require

class db:
    def stop(self):
        startTime = time()
        self.running=False
        self.writeAll()
        self.file.close()
        self.indexFile.close()
        print(f"{self.fileName} closed: {time()-startTime}")
        
    def __init__(self,fileName):
        startTime = time()
        self.running=True
        self.writing=False
        self.fileName = "db/"+fileName
        self.content={}
        self.index={}
        self.queWrite = Queue()
        self.queAlter = Queue()
        
        if not path.exists("db"):
            makedirs("db")
        # Loading db data
        self.loadMetaData()
        
        # Opening most required files
        p = f"{self.fileName}{self.ver}.bin"
        self.file = open(p, 'r+b') if path.exists(p) else open(p, 'w+b')
        p = f"{self.fileName}{self.ver}_index.bin"
        self.indexFile = open(p, 'r+b') if path.exists(p) else open(p, 'w+b')
        
        # Load data
        self.loadIndex()
        
        # Run bg worker
        executor.submit(self.run)
        print(f"{self.fileName}{self.ver} opened: {time()-startTime}")
            
    def loadIndex(self):
        try:
            self.indexFile.seek(0)
            self.index = pickle.load(self.indexFile)
        except EOFError:
            pass
    
    def saveIndex(self):
        iVer = 0 if self.iVer else 1
        with open(f"{self.fileName}{iVer}.bin", "wb") as indexFile:
            pickle.dump(self.index, f, protocol=pickle.HIGHEST_PROTOCOL)
        self.iVer = iVer
        print("WriteAll Complete")
    
        self.indexFile.close()
        p = f"{self.fileName}_index.bin"
        self.indexFile = open(p, 'w+b')
        pickle.dump(self.index, self.indexFile)
    
    def loadMetaData(self):   
        if path.exists(f"{self.fileName}_metaData.bin"):
            with open(f"{self.fileName}_metaData.bin", "rb") as metaData_file:
                self.ver = pickle.load(metaData_file)
        else:
            self.ver = 0
    
    def saveMetaData(self):   
        with open(f"{self.fileName}_metaData.bin", "wb") as metaData_file:
            pickle.dump(self.ver,metaData_file)
    
    def write(self,datas):
        global executor
        self.M_write(datas)
        self.queWrite.put(datas)
    def M_write(self,datas):
        datas=[datas] if type(datas) != list else datas
        for data in datas:
            if data.key in self.index.keys():
                raise ValueError(f"An item of the key already Exists key:{data.key}")
            self.content[data.key] = data
    def F_write(self,datas=None):
        if datas:  
            # datas=datas if hasattr(datas, '__iter__') else [datas]
            for data in datas:
                with open(f"{self.fileName}{self.ver}.bin", "ab") as f:
                    if data.key in self.index.keys():
                        print(f"An item of the key already Exists key:{data.key}")
                        continue
                    self.index[data.key]=f.tell()
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        else:
            # Write to file
            with open(f"{self.fileName}{self.ver}.bin", "ab") as f:
                while not self.queWrite.empty():
                    data = self.queWrite.get(False)
                    if data.key in self.index.keys():
                        print(f"An item of the key already Exists key:{data.key}")
                        continue
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
    def writeAll(self):
        print("Write All")
        while self.writing:
            sleep(.001)
        self.index= {}
        ver = 0 if self.ver else 1
        with open(f"{self.fileName}{ver}.bin", "wb") as f:
            for key,data in self.content.items():
                if data.delete:
                    continue
                self.index[key]=f.tell()
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        if path.exists(f"{self.fileName}{self.ver}.bin"):
            with open(f"{self.fileName}{self.ver}.bin", "rb") as file:
                with open(f"{self.fileName}{ver}.bin", "wb") as f:
                    while True:
                        try:
                            data = pickle.load(file)
                        except EOFError:
                            break
                        if data.delete or data.key in self.index.keys():
                            continue
                        self.index[data.key]=f.tell()
                        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        self.saveIndex()
        self.ver = ver
        self.saveMetaData()
        print("WriteAll Complete")
    
    def readAll(self):
        while self.writing:
            sleep(.1)
        if path.exists(f"{self.fileName}{self.ver}.bin"):
            with open(f"{self.fileName}{self.ver}.bin", "rb") as file:
                while True:
                    try:
                        offset = file.tell()
                        data = pickle.load(file)
                    except EOFError:
                        break
                    if data.key not in self.index.keys():
                        self.index[data.key] = offset
                    if data.key not in self.content.keys():
                        self.content[data.key] = data
                        
    def alter(self,index,field,value):
        self.M_alter()
        self.queAlter.put((index,field,value))
    def M_alter(self,index,field,value):
            setattr(self.content[index],field,value)
    def F_alter(self,index,field,value):
        with open(f"{self.fileName}{self.ver}.bin", "r+b") as f:
            f.seek(self.index[index])
            data = pickle.load(f)
            setattr(data,field,value)
            f.seek(self.index[index])
            pickle.dump(data, f)
    
    def delete(self,index):
        with open(f"{self.fileName}{self.ver}.bin", "r+b") as f:
            f.seek(self.index[index])
            data = pickle.load(f)
            data.delete=True
            f.seek(self.index[index])
            pickle.dump(data, f)
            self.content[index]=data
    
    def readByIndex(self,index):
        if path.exists(f"{self.fileName}{self.ver}.bin"):
            with open(f"{self.fileName}{self.ver}.bin", "rb") as f:
                f.seek(self.index[index])
                data = pickle.load(f)
                return data

    def printIndex(self):
        print(self.index)
        
    def run(self):
        while self.running or self.writing:
            if self.queAlter.empty() and self.queWrite.empty():
                self.writing = False
                sleep(.01)
                continue
            self.writing = True
            if not self.queWrite.empty():
                print("Write")
                self.F_write()
                print("Write COmplete")
            if not self.queAlter.empty():
                self.F_alter(self.queAlter.get())
        print("\nFalse")
        self.writing= False
    
def run():
    worldDb = db("world")

    startTime = time()
    makeCoords(worldDb,0,0,10000,10000,"FFFF")
    print(f"Assign Time : {(time()-startTime)}ms")

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