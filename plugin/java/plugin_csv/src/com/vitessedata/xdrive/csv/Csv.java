package com.vitessedata.xdrive.csv;

import com.vitessedata.xdrive.Xdrive2 ;
import com.vitessedata.xdrive.XdriveUtil ;

public class Csv {

    private int ncol;
    private int nextcol;
    private Xdrive2.ColumnDesc[] coldesc;
    private Xdrive2.XCol[] cols;

    private Xdrive2.WriteRequest wreq;

    public void read(Xdrive2.ReadRequest rreq) throws Exception {


    }

    public void sizeMeta(Xdrive2.SizeMetaRequest szreq) throws Exception {


    }


    public void sample(Xdrive2.SampleRequest req) throws Exception {


    }

    public int write(Xdrive2.XCol col) throws Exception {

        return 0;
    }

    public void writeRequest(Xdrive2.WriteRequest req) throws Exception {

    }

}
