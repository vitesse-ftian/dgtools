package com.vitessedata.xdrive.csv;

import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.nio.file.attribute.*;

import com.vitessedata.xdrive.Xdrive2 ;
import com.vitessedata.xdrive.XdriveUtil ;

import org.apache.commons.csv.*;

public class Csv {

    private int ncol;
    private int nextcol;
    private Xdrive2.ColumnDesc[] coldesc;
    private Xdrive2.XCol[] cols;

    private Xdrive2.WriteRequest wreq;
    private CSVPrinter csvprinter;
    private FileWriter out;

    private String base_path;

    public void config(String base_path) {
        this.base_path = base_path;
        
    }

    public String configFilespec(Xdrive2.FileSpec fspec) {
        String filepath = fspec.getPath();
        int idx = filepath.indexOf("/", 1);
        String path = base_path + filepath.substring(idx);
        return path;
    }

    private List<Path> glob(String localpath, String globpath) throws IOException {
        String pattern = "regex:" + globpath;
        final PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(pattern);
        List<Path> paths = new ArrayList<Path>();

        Files.walkFileTree(Paths.get(localpath), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    if (pathMatcher.matches(path)) {
                        paths.add(path);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

        return paths;
                    
    }


    public void readfile(Xdrive2.ReadRequest rreq, Path p) throws Exception {

        Reader in = null;

        Xdrive2.XCol.Builder[] xcolb = new Xdrive2.XCol.Builder[coldesc.length];
        for (int i = 0; i < coldesc.length; i++) {
            xcolb[i] = Xdrive2.XCol.newBuilder();
            xcolb[i].setColname(coldesc[i].getName());
        }

        try {
            in = new FileReader(p.toFile());
            int nrow = 0;
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withNullString("").parse(in);
            for (CSVRecord record : records) {
                nrow++;
                for (int i = 0 ; i < ncol ; i++) {
                    Xdrive2.ColumnDesc desc = coldesc[i];
                    switch (Xdrive2.SpqType.forNumber(desc.getType())) {
                    case BOOL:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addI32Data(0);
                            } else {

                                boolean v = Boolean.parseBoolean(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addI32Data(v ? 1: 0);

                            }
                        }
                        break;
                    case INT16:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addI32Data(0);
                            } else {
                                int s = (int) Short.parseShort(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addI32Data(s);
                            } 
                        }
                        break;
                    case INT32:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addI32Data(0);
                            } else {
                                int s = Integer.parseInt(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addI32Data(s);
                            } 
                        }
                        break;
                    case INT64:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addI64Data(0);
                            } else {
                                long v = Long.parseLong(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addI64Data(v);
                            }
                        }
                        break;
                    case FLOAT:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addF32Data(0);
                            } else {
                                float f = Float.parseFloat(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addF32Data(f);
                            } 
                        }
                        break;
                    case DOUBLE:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addF64Data(0);
                            } else {
                                double v = Double.parseDouble(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addF64Data(v);
                            } 
                        }
                        break;
                    case DATE:
                    case TIME_MILLIS:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);                                
                                xcolb[i].addI32Data(0);
                            } else {
                                int v = Integer.parseInt(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addI32Data(v);
                            } 
                        }
                        break;
                         
                    case TIME_MICROS:
                    case TIMESTAMP_MILLIS:
                    case TIMESTAMPTZ_MILLIS:
                    case TIMESTAMP_MICROS:
                    case TIMESTAMPTZ_MICROS:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);                                
                                xcolb[i].addI64Data(0);
                            } else {
                                long v = Long.parseLong(str);
                                xcolb[i].addNullmap(false);
                                xcolb[i].addI64Data(v);
                            } 
                        }
                        break;                     
   
                    default:
                        {
                            String str = record.get(i);
                            if (str == null) {
                                xcolb[i].addNullmap(true);
                                xcolb[i].addSdata("");
                            } else {
                                xcolb[i].addNullmap(false);
                                xcolb[i].addSdata(str);
                            }
                        }
                        break;
                    }  // switch
                } // for col
                
                if (nrow == 8192) {
                    for (int i = 0 ;i < xcolb.length; i++) {
                        xcolb[i].setNrow(nrow);
                        XdriveUtil.replyXColData(xcolb[i].build());
                    }
                    nrow = 0;
                    for (int i = 0 ; i < xcolb.length ; i++) {
                        xcolb[i] = Xdrive2.XCol.newBuilder();
                        xcolb[i].setColname(coldesc[i].getName());
                    }
                }

            } // for records

            // write the residuals
            if (nrow > 0) {
                System.err.printf("READ: %d rows.\n", nrow);
                for (int i = 0; i < xcolb.length; i++) {
                    xcolb[i].setNrow(nrow);
                    XdriveUtil.replyXColData(xcolb[i].build());
                }
            }
                
            
        } catch (IOException ex) {
            throw ex;
        }
        finally {
            if (in != null) {
                in.close();
            }
        }
    }
    public void read(Xdrive2.ReadRequest rreq) throws Exception {
        ncol = rreq.getColumndescCount();
        coldesc = new Xdrive2.ColumnDesc[ncol];

        for (int j = 0 ; j < rreq.getColumndescCount() ; j++) {
            coldesc[j] = rreq.getColumndesc(j);
        }

        String filepath = rreq.getFilespec().getPath();
        int idx = filepath.indexOf("/", 1);
        String globpath = filepath.substring(idx+1);

        List<Path> paths = glob(base_path, globpath);

        for (Path p: paths) {

            //Integer hashCode = Math.abs(CommonUtils.hash(fs.getPath().hashCode()));
            int hashCode = Math.abs(p.hashCode());
            if (hashCode % rreq.getFragCnt() == rreq.getFragId()) {
                readfile(rreq, p);
            }
        }

        XdriveUtil.replyXColData(null);
    }
    
    public void sizeMeta(Xdrive2.SizeMetaRequest szreq) throws Exception {


    }


    public void sample(Xdrive2.SampleRequest req) throws Exception {


    }

    public int write(Xdrive2.XCol col) throws Exception {


        if (col == null) {
            if (nextcol == 0) {
                //finished
                if (out != null) {
                    out.close();
                }
                return 0;
            } else {
                // error
                if (out != null) {
                    out.close();
                }
                return -1;
            } 
        } else {
            cols[nextcol] = col;
            nextcol++;
            if (nextcol == ncol) {
                // write 

                writeCols();
                nextcol = 0;
            }
            return 0;
        }
    }


    private String genParsedPath(String pathFmt, int fragCnt, int fragId) {
        String path = pathFmt;
        
        path = path.replaceAll("#UUID#", UUID.randomUUID().toString());
        path = path.replaceAll("#SEGCNT#", ""+fragCnt);
        path = path.replaceAll("#SEGID#", ""+fragId);
        
        return path;
    }

    public void writeRequest(Xdrive2.WriteRequest req) throws Exception {

        wreq = req;

        String rpath = configFilespec(req.getFilespec());
        String path = genParsedPath(rpath, req.getFragCnt(), req.getFragId());

        ncol = req.getColumndescCount();
        coldesc = new Xdrive2.ColumnDesc[ncol];

        out = new FileWriter(path);
        csvprinter = new CSVPrinter(out, CSVFormat.DEFAULT);
        
    }

    public void writeCols() throws Exception {
        int nrow = cols[0].getNrow();
        
        for (int row = 0 ; row < nrow; row++) {
            writeOneRow(row);
        }

    }

    public void writeOneRow(int row) throws Exception {
        List<String> data = new ArrayList<String>();

        for (int col = 0 ; col < ncol ; col++) {
            Xdrive2.ColumnDesc desc = wreq.getColumndesc(col);
            Xdrive2.XCol xcol = cols[col]; 

            switch (Xdrive2.SpqType.forNumber(desc.getType())) {

            case BOOL:
                {
                    boolean b = false;
                    if (!xcol.getNullmap(row)) {
                        b = xcol.getI32Data(row) != 0;
                        data.add(Boolean.toString(b));
                    } else {
                        data.add("");
                    }
                }
                break;

            case INT16:
                {
                    short s = 0;
                    if (!xcol.getNullmap(row)) {
                        s = (short) xcol.getI32Data(row);
                        data.add(Short.toString(s));
                    } else {
                        data.add("");
                    }
                }
                break;

            case INT32:
                {
                    int i = 0;
                    if (!xcol.getNullmap(row)) {
                        i = xcol.getI32Data(row);
                        data.add(Integer.toString(i));
                    } else {
                        data.add("");
                    }
                }
                break;

            case INT64:
                {
                    long l = 0;
                    if (!xcol.getNullmap(row)) {
                        l = xcol.getI64Data(row);
                        data.add(Long.toString(l));
                    } else {
                        data.add("");
                    }
                }
                break;

            case FLOAT:
                {
                    float f = 0;
                    if (!xcol.getNullmap(row)) {
                        f = xcol.getF32Data(row);
                        data.add(Float.toString(f));
                    } else {
                        data.add("");
                    }
                }
                break;

            case DOUBLE:
                {
                    double d = 0;
                    if (!xcol.getNullmap(row)) {
                        d = xcol.getF64Data(row);
                        data.add(Double.toString(d));
                    } else {
                        data.add("");
                    }
                }
                break;

            case DATE:
            case TIME_MILLIS:
                {
                    if (!xcol.getNullmap(row)) {
                        int date = xcol.getI32Data(row);
                        data.add(Integer.toString(date));
                    } else {
                        data.add("");
                    }
                }
                break;

            case TIME_MICROS:
            case TIMESTAMP_MILLIS:
            case TIMESTAMPTZ_MILLIS:
            case TIMESTAMP_MICROS:
            case TIMESTAMPTZ_MICROS:
                {
                    if (!xcol.getNullmap(row)) {
                        long date = xcol.getI64Data(row);
                        data.add(Long.toString(date));
                    } else {
                        data.add("");
                    }   
                }
                break;
            default:
                {
                    String s = xcol.getSdata(row);
                    data.add(s);
                }
                break;

            }
        }

        csvprinter.printRecord(data);
        
    }
   
}
