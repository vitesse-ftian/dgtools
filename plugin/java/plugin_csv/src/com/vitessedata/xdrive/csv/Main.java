package com.vitessedata.xdrive.csv;

import java.io.*;
import java.nio.file.*;
import com.vitessedata.xdrive.Xdrive2; 
import com.vitessedata.xdrive.XdriveUtil;

public class Main {
    public static void main(String[] args) { 
        try {

            System.setErr(new PrintStream("/tmp/xdrive_csv.log"));                    
            if (args.length != 1) {
                System.err.println("usage: java com.vitessedata.xdrive.csv.Main rootpath");
                System.exit(1);
            } 
            
            String root_path = args[0];
            Path rpath = FileSystems.getDefault().getPath(root_path);
            if (! rpath.isAbsolute()) {
                System.err.println("rootpath must be absolute");
                System.exit(1);
            }

            XdriveUtil.openXdriveIO();
                
            Xdrive2.OpSpec opspec = XdriveUtil.readOpSpec();
            
            Csv csv = new Csv();
            csv.config(rpath.toString());
                
            switch (opspec.getOp()) {
            case "read":
                XdriveUtil.replyOpStatus(0, "");
                Xdrive2.ReadRequest rreq = XdriveUtil.readReadRequest();
                csv.read(rreq);
                break;
                
            case "size_meta":
                XdriveUtil.replyOpStatus(0, "");
                Xdrive2.SizeMetaRequest szreq = XdriveUtil.readSizeMetaRequest();
                csv.sizeMeta(szreq);                                    
                break;
                
            case "write": 
                XdriveUtil.replyOpStatus(0, "");
                Xdrive2.WriteRequest wreq = XdriveUtil.readWriteRequest();
                XdriveUtil.replyWrite(0, "");
                csv.writeRequest(wreq);
                
                boolean done = false;
                while (!done) { 
                    int errcode = 0;
                    Xdrive2.XCol col = XdriveUtil.readXCol(); 
                    if (col == null || col.getNrow() == 0) { 
                        errcode = csv.write(null);
                        done = true;
                    } else {
                        errcode = csv.write(col);
                    }
                    
                    if (errcode != 0) {
                        XdriveUtil.replyWrite(errcode, "Csv write error");
                        break;
                    } else {
                        XdriveUtil.replyWrite(0, "");
                    }
                }
                break;
                
            default:
                XdriveUtil.replyOpStatus(-1, "csv plugin unknown plugin op " + opspec.getOp()); 
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}

