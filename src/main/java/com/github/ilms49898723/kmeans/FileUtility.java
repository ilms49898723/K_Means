package com.github.ilms49898723.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileUtility {
    public static void mkdir(String name) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.mkdirs(new Path(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void touch(String name) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.create(new Path(name), true).close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void append(String destination, String source) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path in = new Path(source);
            Path out = new Path(destination);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(in))
            );
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.append(out))
            );
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void copyFile(String input, String output) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path in = new Path(input);
            Path out = new Path(output);
            FileUtil.copy(fileSystem, in, fileSystem, out, false, new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void remove(String path) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String removeExtension(String path) {
        int index = path.lastIndexOf('.');
        if (index == -1) {
            return path;
        } else {
            return path.substring(0, index);
        }
    }

    public static String removeSlash(String path) {
        int index = path.lastIndexOf('/');
        if (index == -1) {
            return path;
        } else {
            return path.substring(index + 1);
        }
    }
}
