from time import time
from os import path,makedirs
import pickle

def makeCoords(db,x,y,value):
    for i in range(x):
        for j in range(y):
            db.write(coord(i,j,f"{(i,j)}:{value}"))

class coord:
    def __init__(self, x, y, content,delete=False):
        self.id = (x,y)
        self.content = content
        self.delete = False

class db:
    def __init__(self,fileName):
        self.fileName = "db/"+fileName
        self.content={}
        self.index={}
        
        if not path.exists("db"):
            makedirs("db")
        if path.exists(f"{self.fileName}_index.bin"):
            with open(f"{self.fileName}_index.bin", "rb") as index_file:
                try:
                    while True:
                        data = pickle.load(index_file)
                        self.index[data[0]] = data[1]
                except EOFError:
                    pass
            
        if path.exists(f"{self.fileName}_id.bin"):
            with open(f"{self.fileName}_id.bin", "rb") as id_file:
                self.id_ = pickle.load(id_file)
        else:
            self.id_=0
            
    def nIndex(self):
        self.id_+=1
        return self.id_
        
    def write(self,data):
        # Open the binary file for writing
        with open(f"{self.fileName}.bin", "ab") as f:
            with open(f"{self.fileName}_index.bin", "ab") as index_file:
                id = self.nIndex()
                if hasattr(data,'id'):
                    id = data.id
                    if data.id in self.index.keys():
                        raise ValueError(f"An item of the name id already Exists id:{data.id}")
                self.index[id] = f.tell()
                pickle.dump((id,f.tell()), index_file)
                pickle.dump(data, f)
    
    def writeMany(self,datas):
        # Open the binary file for writing
        with open(f"{self.fileName}.bin", "ab") as f:
            with open(f"{self.fileName}_index.bin", "ab") as index_file:
                for data in datas:
                    id = self.nIndex()
                    if hasattr(data,'id'):
                        id = data.id
                        if data.id in self.index.keys():
                            raise ValueError(f"An item of the name id already Exists id:{data.id}")
                    self.index[id] = f.tell()
                    pickle.dump((id,f.tell()), index_file)
                    pickle.dump(data, f)
        
    def writeAll(self):
        self.readAll()
        self.index= {}
        # Open the binary file for writing
        with open(f"{self.fileName}.bin", "wb") as f:
            with open(f"{self.fileName}_index.bin", "wb") as index_file:
                for id, data in self.content.items():
                    if hasattr(data,'delete') and data.delete:
                        continue
                    self.index[id]=f.tell()
                    pickle.dump((id,f.tell()), index_file)
                    pickle.dump(data, f)
    
    def readAll(self):
        # Open the binary file for Reading
        if path.exists(f"{self.fileName}.bin") and path.exists(f"{self.fileName}_index.bin"):
            with open(f"{self.fileName}.bin", "rb") as file:
                with open(f"{self.fileName}_index.bin", "rb") as index_file:
                    while True:
                        try:
                            data = pickle.load(index_file)
                        except EOFError:
                            break
                        if data[0] not in self.index.keys():
                            self.index[data[0]] = data[1]
                        if data[0] not in self.content.keys():
                            file.seek(self.index[data[0]])
                            self.content[data[0]] = pickle.load(file)
                        
    def alter(self,index,value):
        with open(f"{self.fileName}.bin", "r+b") as f:
            f.seek(self.index[index])
            data = pickle.load(f)
            data.content=value
            f.seek(self.index[index])
            pickle.dump(data, f)
            self.content[index]=data
    
    def delete(self,index):
        with open(f"{self.fileName}.bin", "r+b") as f:
            f.seek(self.index[index])
            data = pickle.load(f)
            data.delete=True
            f.seek(self.index[index])
            pickle.dump(data, f)
            self.content[index]=data
    
    def readIndex(self,index):
        # Open the binary file for reading
        with open(f"{self.fileName}.bin", "rb") as f:
            f.seek(self.index[index])
            data = pickle.load(f)
            return data

def run():
    startTime = time()
    worldDb = db("world")
    print(f"Initialization Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # makeCoords(worldDb,50000,50000,"FFFF")
    
    # print(f"Assign Time : {(time()-startTime)}ms")
    
    # startTime = time()
    # worldDb.write(coord(-2,3,"FFF2"))
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
    
    # print(f"Data: {data.id} , {data.delete}")
    
    # startTime = time()
    # worldDb.writeAll()
    # print(f"WriteAll Time : {(time()-startTime)}ms")
    
    startTime = time()
    data= worldDb.readIndex((2,40000))
    print(f"Read Time : {(time()-startTime)}ms")
    
    print(f"Data: {data.id} , {data.content}")
    
    startTime = time()
    worldDb.alter((2,40000),"EEEE")
    print(f"Alter Time : {(time()-startTime)}ms")
    
    startTime = time()
    data= worldDb.readIndex((2,40000))
    print(f"Read Time : {(time()-startTime)}ms")
    
    print(f"Data: {data.id} , {data.content}")
    
    
    
if __name__ == "__main__":
    run()