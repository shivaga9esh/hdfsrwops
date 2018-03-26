package com.yobi.readwrite;

import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HDFSJavaAPIReadDemo {

    public static void main(String[] args) throws Exception{
        // Impersonates user "root" to avoid performance problems. You should replace it
        // with user that you are running your HDFS cluster with
        System.setProperty("HADOOP_USER_NAME", "hduser1");

        // Path that we need to create in HDFS. Just like Unix/Linux file systems, HDFS file system starts with "/"
        final Path path = new Path("/allyobitutorials/tutorials-links.txt");

        // Uses try with resources in order to avoid close calls on resources
        // Creates anonymous sub class of DistributedFileSystem to allow calling initialize as DFS will not be usable otherwise
        try(final DistributedFileSystem dFS = new DistributedFileSystem() {
            {
                initialize(new URI("hdfs://localhost:54310"), new Configuration());
            }
        };
            // Gets input stream for input path using DFS instance
            final FSDataInputStream streamReader = dFS.open(path);
            // Wraps input stream into Scanner to use high level and sophisticated methods
            final Scanner scanner = new Scanner(streamReader);) {

            System.out.println("File Contents: ");
            // Reads tutorials information from file using Scanner
            while(scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }

        }
    }
}
