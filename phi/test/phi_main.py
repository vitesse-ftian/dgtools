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

xxx = 200
def do_x():
    outrecs = []
    while True:
        rec = vitessedata.phi.NextInput()
        if not rec:
            sys.stderr.write("Py X: end of input\n")
            break

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
        # vitessedata.phi.WriteOutput(outrec) 

        # global xxx
        # xxx += 1

        # if xxx == 100: 
        #     for i in range(xxx):
        #         sys.stderr.write("Py X: outrec " + str(i) + ":" + str(outrecs[i]) + "\n") 
        #         vitessedata.phi.WriteOutput(outrecs[i])
        #     outrecs = []
        #     xxx = 0
        # else:
        #     sys.stderr.write("Py X: cache outrec " + str(outrec) + ", xxx is " + str(xxx) + "\n") 
            

    vitessedata.phi.WriteOutput(None)

if __name__ == '__main__':
    do_x()
