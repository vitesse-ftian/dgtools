import vitessedata.phi
import sys

vitessedata.phi.DeclareTypes('''
//
// BEGIN INPUT TYPES
// a int32
// b float32
// c string
// END INPUT TYPES
//
// BEGIN OUTPUT TYPES
// x int32
// y float32
// z string
// END OUTPUT TYPES
//
''')

def do_x():
    outrecs = []
    while True:
        rec = vitessedata.phi.NextInput()
        if not rec:
            return outrecs

        # sys.stderr.write("Py X: input rec " + str(rec) + "\n")
        outrec = [None, None, None]

        if rec[0] is None:
            outrec[0] = rec[0]
        else:
            outrec[0] = rec[0] * 2

        if rec[1] is None:
            outrec[1] = rec[1]
        else:
            outrec[1] = rec[1] * 2.0

        if rec[2] is None:
            outrec[2] = None 
        else:
            outrec[2] = rec[2] + rec[2] 

        outrecs.append(outrec)
        if len(outrecs) == 100:
            return outrecs
          
if __name__ == '__main__':
    while True:
        xxx = do_x()
        if len(xxx) != 0:
            for x in range(len(xxx)):
                vitessedata.phi.WriteOutput(xxx[x])
        else:
            vitessedata.phi.WriteOutput(None)
            break
