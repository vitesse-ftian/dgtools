import struct, sys
import xdrive_data_pb2

def readXMsg():
    s = sys.stdin.read(4)
    if s is None or s == '':
        return None
    sz = struct.unpack('<i', s)[0] 
    # sys.stderr.write("Py: read a message of size " + str(sz) + "\n")
    xmsg = xdrive_data_pb2.XMsg()
    xmsg.ParseFromString(sys.stdin.read(sz))
    return xmsg

def writeXMsg(xmsg):
    xs = xmsg.SerializeToString()
    sz = len(xs) 
    szstr = struct.pack('<i', sz) 
    sys.stdout.write(szstr)
    sys.stdout.write(xs)

phiTypes = ['bool', 'int32', 'int64', 'float32', 'float64', 'string']

class PhiRt(object):
    def __init__(self):
        self.inMsg = None
        self.numInRecs = 0
        self.currInRec = 0
        self.inTypes = []
        self.outRecs = [None] * 1024
        self.currOutRec = 0
        self.outTypes = []

    def parseTypeLine(self, line):
        fields = line.split()
        if len(fields) < 2:
            return None 

        tp = fields[-1]
        if tp in phiTypes:
            return tp
        else:
            return None 

    def declTypes(self, ss):
        # sys.stderr.write("Py: decltypes " + ss) 
        lines = ss.splitlines()
        # sys.stderr.write("Py: decltypes lines " + str(lines) + "\n") 
        doinput = False
        dooutput = False
        for line in lines:
            if line.find('BEGIN INPUT TYPES') > 0:
                doinput = True
            elif line.find('END INPUT TYPES') > 0:
                doinput = False
            elif line.find('BEGIN OUTPUT TYPES') > 0:
                dooutput = True
            elif line.find('END OUTPUT TYPES') > 0:
                dooutput = False
            else: 
                tp = self.parseTypeLine(line)
                if tp is not None:
                    if doinput:
                        # sys.stderr.write("Py: add in type " + tp + "\n")
                        self.inTypes.append(tp)
                    elif dooutput:
                        # sys.stderr.write("Py: add out type " + tp + "\n")
                        self.outTypes.append(tp)
        # sys.stderr.write("Py: types, in: " + str(self.inTypes) + "\n")
        # sys.stderr.write("Py: types, out: " + str(self.outTypes) + "\n")

    def loadInRecs(self): 
        xmsg = readXMsg()
        if xmsg is None:
            # sys.stderr.write("Py: inMsg is none\n")
            return None
        rs = xmsg.rowset 
        if len(rs.columns) == 0 or rs.columns[0].nrow == 0:
            # sys.stderr.write("Py: inMsg is eos") 
            return None

        self.inMsg = xmsg
        self.numInRecs = rs.columns[0].nrow
        # sys.stderr.write("Py: inMsg has " + str(self.numInRecs) + " rows.\n")
        self.currInRec = 0

    def nextInput(self):
        rs = self.inMsg.rowset 
        r = self.currInRec
        ncol = len(self.inTypes)
        ret = [] 
        for c in range(ncol):
            col = rs.columns[c]
            if col.nullmap[r]: 
                ret.append(None)
            else:
                if self.inTypes[c] == 'bool':
                    if col.i32data[r] == 0:
                        ret.append(False) 
                    else:
                        ret.append(True)
                elif self.inTypes[c] == 'int32':
                    ret.append(col.i32data[r])
                elif self.inTypes[c] == 'int64':
                    ret.append(col.i64data[r])
                elif self.inTypes[c] == 'float32':
                    ret.append(col.f32data[r])
                elif self.inTypes[c] == 'float64':
                    ret.append(col.f64data[r])
                else:
                    ret.append(col.sdata[r])

        self.currInRec += 1
        # sys.stderr.write("Py: Returning input: " + str(ret) + "\n")
        return ret

    def fillCol(self, col, c): 
        col.nrow = self.currOutRec
        if self.outTypes[c] == 'bool':
            for r in range(self.currOutRec):
                if self.outRecs[r][c] is None:
                    col.nullmap.append(True)
                    col.i32data.append(0)
                else:
                    col.nullmap.append(False)
                    if self.outRecs[r][c]:
                        col.i32data.append(1)
                    else:
                        col.i32data.append(0)
        elif self.outTypes[c] == 'int32':
            for r in range(self.currOutRec):
                if self.outRecs[r][c] is None:
                    col.nullmap.append(True)
                    col.i32data.append(0)
                else:
                    col.nullmap.append(False)
                    col.i32data.append(self.outRecs[r][c])
        elif self.outTypes[c] == 'int64':
            for r in range(self.currOutRec):
                if self.outRecs[r][c] is None:
                    col.nullmap.append(True)
                    col.i64data.append(0)
                else:
                    col.nullmap.append(False)
                    col.i64data.append(self.outRecs[r][c])
        elif self.outTypes[c] == 'float32':
            for r in range(self.currOutRec):
                if self.outRecs[r][c] is None:
                    col.nullmap.append(True)
                    col.f32data.append(0)
                else:
                    col.nullmap.append(False)
                    col.f32data.append(self.outRecs[r][c])
        elif self.outTypes[c] == 'float64':
            for r in range(self.currOutRec):
                if self.outRecs[r][c] is None:
                    col.nullmap.append(True)
                    col.f64data.append(0)
                else:
                    col.nullmap.append(False)
                    col.f64data.append(self.outRecs[r][c])
        else:
            for r in range(self.currOutRec):
                if self.outRecs[r][c] is None:
                    col.nullmap.append(True)
                    col.sdata.append("")
                else:
                    col.nullmap.append(False)
                    col.sdata.append(self.outRecs[r][c])
   
    def writeOutRecs(self, rs):
        if self.currOutRec == 0:
            return None
        ncol = len(self.outTypes)
        for c in range(ncol):
            col = rs.columns.add()
            self.fillCol(col, c)
        return rs

phirt = PhiRt()

def DeclareTypes(s):
    phirt.declTypes(s)

def NextInput():
    if phirt.inMsg is None: 
        phirt.loadInRecs() 
        if phirt.inMsg is None:
            return None

    if phirt.currInRec < phirt.numInRecs:
        # sys.stderr.write("Py: nextInput return " + str(phirt.currInRec) + " out of " + str(phirt.numInRecs) + " rows.\n")
        return phirt.nextInput()
    else:
        # sys.stderr.write("Py: nextInput msg exhausted " + str(phirt.currInRec) + " out of " + str(phirt.numInRecs) + " rows.\n")
        FlushOutput(0) 
        phirt.inMsg = None 
        return NextInput()

def WriteOutput(r):
    # sys.stderr.write("Py: WriteOutput " + str(r) + "\n")
    if r is None:
        FlushOutput(0)
        FlushOutput(-1)
    else:
        if phirt.currOutRec < 1024:
            phirt.outRecs[phirt.currOutRec] = r
            phirt.currOutRec += 1
        else:
            FlushOutput(1)
            WriteOutput(r)

def FlushOutput(flag):
    xmsg = xdrive_data_pb2.XMsg()
    xmsg.flag = flag
    if flag >= 0:
        phirt.writeOutRecs(xmsg.rowset)
        phirt.currOutRec = 0
    writeXMsg(xmsg)
