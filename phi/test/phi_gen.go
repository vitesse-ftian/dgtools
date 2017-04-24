
//
// DO NOT EDIT
// Code is GENERATED BY VitesseData Phi Code Gen.  DO NOT EDIT
// 

package main

import (
	"vitessedata/phi/datatype"
	"vitessedata/phi/phirun"
	"vitessedata/phi/proto/xdrive"
)

func Log(msg string, args ...interface{}) {
	phirun.Log(msg, args...)
}

type InRecord struct {
	a datatype.OptionalInt32
	b datatype.OptionalFloat32
	c datatype.OptionalString
}

func (r *InRecord) Get_a() (int32, bool) {
	return r.a.Get()
}

func (r *InRecord) Set_a(v int32) {
	r.a.Set(v)
}

func (r *InRecord) Set_a_Null() {
	r.a.SetNull()
}

func (r *InRecord) Get_b() (float32, bool) {
	return r.b.Get()
}

func (r *InRecord) Set_b(v float32) {
	r.b.Set(v)
}

func (r *InRecord) Set_b_Null() {
	r.b.SetNull()
}

func (r *InRecord) Get_c() (string, bool) {
	return r.c.Get()
}

func (r *InRecord) Set_c(v string) {
	r.c.Set(v)
}

func (r *InRecord) Set_c_Null() {
	r.c.SetNull()
}


type OutRecord struct {
	x datatype.OptionalInt32
	y datatype.OptionalFloat32
	z datatype.OptionalString
}

func (r *OutRecord) Get_x() (int32, bool) {
	return r.x.Get()
}

func (r *OutRecord) Set_x(v int32) {
	r.x.Set(v)
}

func (r *OutRecord) Set_x_Null() {
	r.x.SetNull()
}

func (r *OutRecord) Get_y() (float32, bool) {
	return r.y.Get()
}

func (r *OutRecord) Set_y(v float32) {
	r.y.Set(v)
}

func (r *OutRecord) Set_y_Null() {
	r.y.SetNull()
}

func (r *OutRecord) Get_z() (string, bool) {
	return r.z.Get()
}

func (r *OutRecord) Set_z(v string) {
	r.z.Set(v)
}

func (r *OutRecord) Set_z_Null() {
	r.z.SetNull()
}



type phi_runtime_t struct {
	inRecs []InRecord
	currInRec int32

	outRecs []*OutRecord
	currOutRec int32
}


func (rt *phi_runtime_t) loadInRecs(rs *xdrive.XRowSet) {
	cols := rs.Columns
	nrow := cols[0].Nrow
	rt.inRecs = make([]InRecord, nrow, nrow)
	for r := int32(0); r < nrow; r++ {
		if cols[0].Nullmap[r] {
			rt.inRecs[r].Set_a_Null()
		} else {
			rt.inRecs[r].Set_a(cols[0].I32Data[r])
		}
	}
	for r := int32(0); r < nrow; r++ {
		if cols[1].Nullmap[r] {
			rt.inRecs[r].Set_b_Null()
		} else {
			rt.inRecs[r].Set_b(cols[1].F32Data[r])
		}
	}
	for r := int32(0); r < nrow; r++ {
		if cols[2].Nullmap[r] {
			rt.inRecs[r].Set_c_Null()
		} else {
			rt.inRecs[r].Set_c(cols[2].Sdata[r])
		}
	}
}


func (rt *phi_runtime_t) writeOutRecs() *xdrive.XRowSet {
	if rt.currOutRec == 0 {
		return nil
	}
	var rs xdrive.XRowSet
	nrow := rt.currOutRec
	rs.Columns = make([]*xdrive.XCol, 3, 3)
	rs.Columns[0] = new(xdrive.XCol)
	rs.Columns[0].Colname = "x"
	rs.Columns[0].Nrow = nrow
	rs.Columns[0].Nullmap = make([]bool, nrow, nrow)
	rs.Columns[0].I32Data = make([]int32, nrow, nrow)
	for r := int32(0); r < nrow; r++ {
		v, ok := rt.outRecs[r].Get_x()
		if !ok {
			rs.Columns[0].Nullmap[r] = true
		} else {
			rs.Columns[0].Nullmap[r] = false
			rs.Columns[0].I32Data[r] = v
		}
	}

	rs.Columns[1] = new(xdrive.XCol)
	rs.Columns[1].Colname = "y"
	rs.Columns[1].Nrow = nrow
	rs.Columns[1].Nullmap = make([]bool, nrow, nrow)
	rs.Columns[1].F32Data = make([]float32, nrow, nrow)
	for r := int32(0); r < nrow; r++ {
		v, ok := rt.outRecs[r].Get_y()
		if !ok {
			rs.Columns[1].Nullmap[r] = true
		} else {
			rs.Columns[1].Nullmap[r] = false
			rs.Columns[1].F32Data[r] = v
		}
	}

	rs.Columns[2] = new(xdrive.XCol)
	rs.Columns[2].Colname = "z"
	rs.Columns[2].Nrow = nrow
	rs.Columns[2].Nullmap = make([]bool, nrow, nrow)
	rs.Columns[2].Sdata = make([]string, nrow, nrow)
	for r := int32(0); r < nrow; r++ {
		v, ok := rt.outRecs[r].Get_z()
		if !ok {
			rs.Columns[2].Nullmap[r] = true
		} else {
			rs.Columns[2].Nullmap[r] = false
			rs.Columns[2].Sdata[r] = v
		}
	}

	return &rs
}


var phirt phi_runtime_t 

func init() {
	phirt.outRecs = make([]*OutRecord, 1024)
}

func NextInput() *InRecord {
	if phirt.inRecs == nil {
		inMsg, err := phirun.ReadXMsg()
		if err != nil || inMsg == nil || inMsg.Flag == -1 || inMsg.Rowset == nil {
			// End of input stream.
			return nil
		}
		// if inMsg.Rowset != nil, it must has col data.
		phirt.loadInRecs(inMsg.Rowset)
	}
	
	if phirt.currInRec < int32(len(phirt.inRecs)) {
		ret := &phirt.inRecs[phirt.currInRec]
		phirt.currInRec += 1
		return ret
	} else {
		// All InRec from inMsg exhausted.  We will flush outRec.
		Log("Flush Output Because We Need More Input.\n")
		FlushOutput(0)
		phirt.inRecs = nil
		return NextInput()
	}
}



func WriteOutput(r *OutRecord) {
	if (r == nil) {
		Log("Flush Output Because Done.\n")
		FlushOutput(0)
		FlushOutput(-1)
	} else {
		if phirt.currOutRec < 1024 {
			phirt.outRecs[phirt.currOutRec] = r
			phirt.currOutRec += 1
			return
		} else {
			Log("Flush Output Because Output Buffer Full.\n")
			FlushOutput(1)
			WriteOutput(r)
		}
	}
}

func FlushOutput(flag int64) {
	var msg xdrive.XMsg
	msg.Flag = flag
	Log("Flush output, flag is %!v(MISSING), currOutRec is %!v(MISSING).\n", flag, phirt.currOutRec)

	if flag >= 0 {
		msg.Rowset = phirt.writeOutRecs()
		phirt.currOutRec = 0
	}
	phirun.WriteXMsg(&msg)
}

