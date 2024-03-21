package it.polimi.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * The HadoopFileManager class is an abstract class used to manage the HDFS file system.
 * It contains the HDFS URI, the file system and the buffer size.
 * It is extended by HadoopCoordinator and HadoopWorker classes.
 */
public abstract class HadoopFileManager {
    protected final String HDFS_URI;
    protected FileSystem fs;
    protected static Logger logger;
    protected final int BUFFER_SIZE;
    /**
     * The constructor creates a new HadoopFileManager.
     * @param address represents the address of hdfs.
     * @param BUFFER_SIZE represents the buffer size.
     * @throws IOException if the file system cannot be initialized.
     */

    public HadoopFileManager(String address, int BUFFER_SIZE) throws IOException {
        this.HDFS_URI = address;
        this.BUFFER_SIZE = BUFFER_SIZE;
        try {
            fs = initialize();
        } catch (IOException e) {
            logger.error(e);
            throw new IOException("Not possible to connect to the HDFS server. Check the address of the server and if it is running!\nCheck also if files exist!\n" + e.getMessage());
        }
    }
    /**
     * The initialize method initializes the file system.
     * @return the file system.
     * @throws IOException if the file system cannot be initialized.
     */
    protected FileSystem initialize() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return FileSystem.newInstance(conf);
    }
    /**
     * The closeFileSystem method closes the file system.
     */
    public void closeFileSystem() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                logger.error(Thread.currentThread().getName() + ": Error closing the file system: " + e.getMessage());
                System.out.println(Thread.currentThread().getName() + ": Error closing the file system: " + e.getMessage());
            }
        }
    }


}