package it.polimi.worker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

// I want to read the values of the keys from a remote file using HDFS
public class KeyReader {
    // Method to read the keys from a remote file using HDFS
    public List<String> readKeys(String path) {
        List<String> keys = new ArrayList<>();
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(URI.create(path), conf);
            Path filePath = new Path("/user/your_username/new_directory"); // path
            if (fs.exists(filePath)) {
                FSDataInputStream inputStream = fs.open(filePath);

                // Use BufferedReader to read the content line by line
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;

                // Read and print each line
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }

                // Close the input stream and file system
                reader.close();
                inputStream.close();
                fs.close();
            }
            else {
                System.out.println("File does not exist: " + filePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return keys;
    }

    public static void main(String[] args) {
        KeyReader keyReader = new KeyReader();
        List<String> keys = keyReader.readKeys("hdfs://localhost:5001/C:\\Users\\morel\\IdeaProjects\\DS_Project\\step1_5001\\file_1.csv");
        System.out.println(keys);
    }
}
