package com.github.ilms49898723.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Cleaner {
    public static void remove(String dir) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.delete(new Path(dir), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
