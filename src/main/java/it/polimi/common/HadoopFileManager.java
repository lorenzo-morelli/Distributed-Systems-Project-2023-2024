package it.polimi.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public abstract class HadoopFileManager{
    protected String HDFS_URI = "hdfs://localhost:9000";
    protected FileSystem fs;
    protected static Logger logger;

    public HadoopFileManager(String address) throws IOException{
        setHDFS_URI(address);
        try{
        fs = initialize();
        }catch(IOException e){
            logger.error(e);
            throw new IOException("Not possible to connect to the HDFS server. Check the address of the server and if it is running!\nCheck also if files exist!\n" + e.getMessage());        }
    }

    private void setHDFS_URI(String newURI) {
        HDFS_URI = newURI;
    }
    protected FileSystem initialize() throws IOException{
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem filesystem = FileSystem.newInstance(conf);
		return filesystem;
	}
    public void closeFileSystem(){
        if (fs!=null) {
            try {
                fs.close();
            } catch (IOException e) {
                logger.error(Thread.currentThread().getName()+ ": Error closing the file system: " + e.getMessage());
                System.out.println(Thread.currentThread().getName()+ ": Error closing the file system: " + e.getMessage());
            }
        }
    }
    

}