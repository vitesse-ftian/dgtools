select dg_utils.transducer($PHI$PhiExec python2
import vitessedata.phi

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
    while True:
        rec = vitessedata.phi.NextInput()
        if not rec:
            break

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

        vitessedata.phi.WriteOutput(outrec)

    vitessedata.phi.WriteOutput(None)

if __name__ == '__main__':
    do_x()

$PHI$,
t.*),
dg_utils.transducer_column_int4(1) as i32,
dg_utils.transducer_column_float4(2) as f32,
dg_utils.transducer_column_text(3) as t
from (
    select i::int, i::float4, i::text from generate_series(1, 2000) i
) t
;
