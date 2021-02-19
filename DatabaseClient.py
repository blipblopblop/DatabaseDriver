#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
import struct
import re
from . import exception as ex
from . import packet

class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    # initialize self.tb = (table_number, table)
    # ex) the number of rows in the table with table_number 1 is at self.tableRows[0]
    def __init__(self, tables):
        self.tableNames = []
        self.tb = []
        self.socket = None

        for tb_num, tb_ele in enumerate(tables):
            if(tb_ele[0] in self.tableNames):
                raise ValueError("Duplicate table name")
            else:
                self.tableNames.append(tb_ele[0])
            self.checkTableName(tb_ele[0])    
            self.checkColumn(tb_ele[1], tb_ele[0])
            self.tb.append((tb_num+1, tb_ele))
        
        return

    def checkTableName(self, tb_name):
        if(type(tb_name) != str):
            raise TypeError("Illegal table name specifier")
        if(any(map(str.isdigit, tb_name))):
            raise TypeError("Invalid table name")

        regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
        if(regex.search(tb_name) != None):
            raise ValueError("Invalid table name")

    def checkColumn(self, columns, current_table):
        valid_types = [str, int, float]
        col_names = []
        for col in columns:
            col_names.append(col[0])
        
        col_names_set = set(col_names)
        if(len(col_names) != len(col_names_set)):
            raise ValueError("Duplicate column name")
        for col in columns:
            
            if(type(col[0]) != str):
                raise TypeError("Illegal column name specifier")
            
            if(col[0] == 'id'):
                raise ValueError("Column name is 'id'")

            if(any(map(str.isdigit, col[0]))):
                raise ValueError("Invalid column name")

            regex = re.compile('[@!#$%^&*()<>?/\|}{~:]')
            if(regex.search(col[0]) != None):
                print("col[0]", col[0])
                raise ValueError("Invalid column name")


            if(col[1] not in valid_types):
                if(type(col[1]) == str):
                    if(col[1] not in self.tableNames):
                        raise ex.IntegrityError("Invalid foreign reference")
                    if(col[1] == current_table):
                        raise ex.IntegrityError("Invalid foreign reference")
                    
                else:
                    raise ValueError("Illegal column type")
        return


    def isValidTable(self, tb_name):
        for table in self.tb:
            if(tb_name == table[1][0]):
                return True
        return False
    
    # returns the table that matches with the table_name
    # returned table will have (table_name, (columns))
    # return None if not found
    def getTable(self, table_name):
        for tb_ele in self.tb:
            if(table_name == tb_ele[1][0]):    
                return tb_ele
        
        return None

    # check if the table is referring to another table (foreign table)
    # if True, then return True, index of the element that refers
    # to the foriegn table row
    # and the table_number of the foreign table
    def checkForeign(self, table_name, colTypeList):
        for idx, key_name in enumerate(colTypeList):
            for tb_ele in self.tb:
                if(tb_ele[1][0] != table_name and
                    tb_ele[1][0] == key_name):           
                    return True, idx, tb_ele[0]
        
        return False, -1, -1
    
    # pad value with b'\x00\' if it is not aligned by 4 bytes    
    def align(self, val):
        while(len(val) % 4 != 0):
            val = val + b'\x00'
        return val

    # return the packed row data for insertion
    def getPackedElement(self, foreignIdx, idx, key_type, val):
        
        if(key_type == str):
            aligned_str = self.align(val.encode())
            return (struct.pack("!ii", packet.STRING, len(aligned_str)) + aligned_str)
        elif(key_type == int):
            if(foreignIdx == idx):
                #foreign key: the value is the row id of the ref table
                return (struct.pack("!iiq", packet.FOREIGN, 8, val))
            else:
                return (struct.pack("!iiq", packet.INTEGER, 8, val))
        elif(key_type == float):
            return (struct.pack("!iid", packet.FLOAT, 8, val))
        else:
            raise ex.PacketError("invalid key type?")
            
    # given the server response, unpack the byte data to relevant readable data
    # used in GET function
    def unpackRowValues(self, count, data):
        value = []        
        val_startIdx = 16
        for i in range(count):
            val_type = struct.unpack("!i", data[val_startIdx:val_startIdx+4])[0]
            val_size = struct.unpack("!i", data[val_startIdx+4:val_startIdx+8])[0]
            val_startIdx = val_startIdx+8
            if(val_type == 3):
                #str
                buf = (data[val_startIdx:val_startIdx+val_size]).decode()
                buf = buf.replace('\x00', '')
                value.append(buf)
                val_startIdx = val_startIdx + val_size
            elif(val_type == 2):
                #float
                buf = struct.unpack("!d", data[val_startIdx:val_startIdx+val_size])[0]
                value.append(buf)
                val_startIdx = val_startIdx + val_size
            else:
                #int or foreign
                buf = struct.unpack("!q", data[val_startIdx:val_startIdx+val_size])[0]
                value.append(buf)
                val_startIdx = val_startIdx + val_size
        
        return value
        

    def connect(self, host, port):
        try:
            self.socket = socket.socket()
            self.socket.connect((host, port))
            res = self.socket.recv(4096)
            if(res == 10):
                return False
            else:
                return True
        except socket.error as err:
            print("socket creation failed with error %s" %(err))
            return False


    def close(self):
        reqData = struct.pack("!ii", 6, 1)
        self.socket.sendall(reqData)
        res = self.socket.recv(4096)
        self.socket.close()
        return


    def insert(self, table_name, values):
        command = packet.INSERT
        tableInfo = self.getTable(table_name)
        if(tableInfo == None):
            #BAD_TABLE
            raise ex.PacketError("Invalid Table")                    
    
        table_number = tableInfo[0]
        columns = tableInfo[1][1]
        #print("insert columns: ", columns)
        val_length = len(columns)
        if(val_length != len(values)):
            raise ex.PacketError("Error with the Number of Value")        
        colTypeList = []
        #colNameList = []
        for col_name, col_type in columns:
            #colNameList.append(col_name) No need for now
            colTypeList.append(col_type)
        foreign, foreignIdx, foreignTableNum = self.checkForeign(table_name, colTypeList)
        if(foreign == True):
            colTypeList[foreignIdx] = int
        
        row_val = ''.encode()
        for idx, val in enumerate(values):

            if(idx != foreignIdx):
                if(isinstance(val, colTypeList[idx]) == False):
                    #BAD_VALUE
                    raise ex.PacketError("Wrong Value Type")
            packed_val = self.getPackedElement(foreignIdx, idx, colTypeList[idx], val)
            row_val = row_val + packed_val
        
        request = struct.pack("!iii", command, table_number, val_length) 
        self.socket.sendall(request+row_val)
        res = self.socket.recv(4096)
        
        if(len(res) == packet.BAD_QUERY):
            res_code = struct.unpack("!i", res)
            #BAD_FOREIGN
            if(res_code[0] == packet.BAD_FOREIGN):
                raise ex.InvalidReference("Invalid Foreign Reference")
        else:    
            res_code, key_id, key_version = struct.unpack("!iqq", res)
            if(res_code == packet.OK):
                return (key_id, key_version)                

    def update(self, table_name, pk, values, version=None):
        command = packet.UPDATE

        if version == None:
            version = 0

        if isinstance(pk, int) == False:
            raise ex.PacketError("Invalid ID arg")
        if isinstance(version, int) == False:
            raise ex.PacketError('Invalid Version type')

        tableInfo = self.getTable(table_name)
        if(tableInfo == None):
            raise ex.PacketError("Invalid Table")    
        
        table_number = tableInfo[0]
        columns = tableInfo[1][1]
        numColumns = len(columns)      
        colTypeList = []
        for col_name, col_type in columns:
            colTypeList.append(col_type)

        foreign, foreignIdx, foreignTableNum = self.checkForeign(table_name, colTypeList)
        if(foreign == True):
            colTypeList[foreignIdx] = int
        
        row_val = ''.encode()

        for idx, val in enumerate(values):
            if(idx != foreignIdx):
                if(isinstance(val, colTypeList[idx]) == False):
                    raise ex.PacketError("Wrong Value Type")

            key_type = colTypeList[idx]
            if(key_type == str):
                packed_val = struct.pack("!ii{}s".format(len(val)), packet.STRING, len(val), val.encode())
            elif(key_type == int):
                if(foreignIdx == idx):
                    #foreign key: the value is the row id of the ref table
                    packed_val = (struct.pack("!iiq", packet.FOREIGN, 8, val))
                else:
                    packed_val = (struct.pack("!iiq", packet.INTEGER, 8, val))
            elif(key_type == float):
                packed_val = (struct.pack("!iid", packet.FLOAT, 8, val))
            else:
                raise ex.packetError("invalid key type?")
            row_val = row_val + packed_val

        request = struct.pack("!iiqqi", packet.UPDATE, table_number, pk, version, len(values)) 
        self.socket.sendall(request+row_val)

        #unpked = struct.unpack("!iiqqiii" + str(lenOne) + "sii" + str(lenTwo) + "siidiiq", request+row_val)  

        res = self.socket.recv(4096)

        if(len(res) == 4):
            res_code = struct.unpack("!i", res)
            if(res_code[0] == packet.BAD_REQUEST):
                print("malformed packet")
            if(res_code[0] == packet.BAD_FOREIGN):
                raise ex.InvalidReference("Invalid Foreign Reference")
            if(res_code[0] == packet.NOT_FOUND):
                raise ex.ObjectDoesNotExist("The Row not Found")
            if(res_code[0] == packet.TXN_ABORT):
                raise ex.TransactionAbort("Automic Update has Failed")
        else:    
            res, version = struct.unpack("!iq", res[:12])
            if(res == packet.OK):
                return (version)
            else:
                print("unknown response")

    def drop(self, table_name, pk):
        command = packet.DROP
        if isinstance(pk, int) == False:
            raise ex.PacketError("Invalid ID arg")

        tableInfo = self.getTable(table_name)
        if(tableInfo == None):
            raise ex.PacketError("Invalid Table")    
        table_number = tableInfo[0]

        request = struct.pack("!iiq", packet.DROP, table_number, pk) 
        self.socket.sendall(request) 
        res = self.socket.recv(4096)

        if(len(res) == 4):
            res_code = struct.unpack("!i", res)
            if(res_code[0] == packet.NOT_FOUND):
                raise ex.ObjectDoesNotExist("The Row not Found")


    def get(self, table_name, pk):
        TableInfo = self.getTable(table_name)
                    
        if(TableInfo == None):
            raise ex.PacketError("Invalid Page Name")

        if(type(pk) != int):
            raise ex.PacketError("Invalid ID arg")        

        req_command = packet.GET
        table_number = TableInfo[0]

        request = struct.pack("!iiq", req_command, table_number, pk)
        self.socket.sendall(request)
        
        response = self.socket.recv(4096)
        if(len(response) == 4):
            res = struct.unpack("!i", response)
            if(res[0] == packet.NOT_FOUND):
                raise ex.ObjectDoesNotExist("non-existent row")
        else:
            res, version = struct.unpack("!iq", response[:12])
            if(res == packet.OK):
                count = struct.unpack("!i", response[12:16])[0]
                row = self.unpackRowValues(count, response)
                return (row, version)
    
    def scan(self, table_name, op, column_name=None, value=None):
        
        if(type(op) != int):
            raise ex.PacketError("Invalid operator type")

        if(op > packet.operator.GT or op < packet.operator.AL):
            raise ex.PacketError("Operator does not exist")
        
        if(op != packet.operator.AL and column_name == None):
            raise ex.PacketError("Missing column name")
        
        if(column_name != None and value == None):
            raise ex.PacketError("Missing right operand value")
        
        TableInfo = self.getTable(table_name)
        if(TableInfo == None):
            raise ex.PacketError("Table name does not exist")
        
        command = packet.SCAN
        table_number = TableInfo[0]
        col_count = 0

        if(op == packet.operator.AL):
            valid_colName = True
        else:            
            if(type(column_name) != str):
                raise ex.PacketError("Invalid column name")   
            
            valid_colName = False
            foreign = False
            foreignIdx = -1 #this field is no need for now
            columns = TableInfo[1]
            

            if(column_name == "id"):
                valid_colName = True
                foreign = True
            else:
                for idx, column in enumerate(TableInfo[1][1]):
                    if(column[0] == column_name):
                        valid_colName = True
                        if(self.isValidTable(column[1]) == True):
                            foreign = True
                            foreignIdx = idx
                        if(column[1] != type(value)):
                            raise ex.PacketError("Right operand does not match column type")
                        break
                for count, col in enumerate(columns[1]):
                    col_count = count + 1
                    if(col[0] == column_name):
                        break

            if(valid_colName == False):
                raise ex.PacketError("Column name does not exist")
            
            if(foreign == True and (op != packet.operator.EQ and op != packet.operator.NE)):
                raise ex.PacketError("Invalid operator for foreign key")

            if(foreign == True and type(value) != int):
                raise ex.PacketError("Invalid operand type for foreign key")
            
            

        
        request = struct.pack("!iiii", command, table_number, col_count, op)
        val = ''.encode()
        if(op == packet.operator.AL):
            val = struct.pack("iii", packet.NULL, 0, packet.NULL)
        else:
            if(type(value) == int):
                if(foreign == True):
                    val = struct.pack("!iiq", packet.FOREIGN, 8, value)
                else:             
                    val = struct.pack("!iiq", packet.INTEGER, 8, value)
            elif(type(value) == float):
                val = struct.pack("!iid", packet.FLOAT, 8, value)
            elif(type(value) == str):
                aligned_str = self.align(value.encode())
                val = struct.pack("!ii", packet.STRING, len(aligned_str)) + aligned_str 
            else:
                raise ex.PacketError("invalid key type?")

      
        self.socket.sendall(request+val)
        response = self.socket.recv(4096)
        res = struct.unpack("!i", response[:4])
        if(res[0] == 1):
            count = struct.unpack("!i", response[4:8])[0]
            start_idx = 8
            ids = []       
            for i in range(count):
                ids.append( struct.unpack("!q", response[start_idx:start_idx+8])[0])
                start_idx = start_idx + 8
            return ids

