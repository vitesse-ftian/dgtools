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
    private List<String[]> data;

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


    public void read(Xdrive2.ReadRequest rreq) throws Exception {
        ncol = rreq.getColumndescCount();
        coldesc = new Xdrive2.ColumnDesc[ncol];

        String filepath = rreq.getFilespec().getPath();
        int idx = filepath.indexOf("/", 1);
        String globpath = filepath.substring(idx+1);

        List<Path> paths = glob(base_path, globpath);

        for (Path p: paths) {

            //Integer hashCode = Math.abs(CommonUtils.hash(fs.getPath().hashCode()));
            int hashCode = Math.abs(p.hashCode());
            if (hashCode % rreq.getFragCnt() == rreq.getFragId()) {
                Reader in = new FileReader(p.toFile());
                Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
                for (CSVRecord record : records) {
                    record.get(0);
                    record.get(1);
                }
            }
        }
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
                csvprinter.printRecords(data);
                
                data.clear();
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

}
