package com.github.ilms49898723.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class KMeansMain extends Configured implements Tool {
    public static final int K = 2;
    public static final int MAX_ITER = 1;

    private ArrayList<ArrayList<PointPosition>> mCentroids;

    private void initializeCentroids(String c1, String c2) {
        try {
            mCentroids = new ArrayList<>();
            mCentroids.add(new ArrayList<>());
            mCentroids.add(new ArrayList<>());
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path[] paths = { new Path(c1), new Path(c2) };
            for (int i = 0; i < 2; ++i) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fileSystem.open(paths[i]))
                );
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    String[] tokens = line.split("\\s+");
                    PointPosition pointPosition = new PointPosition();
                    for (String token : tokens) {
                        pointPosition.add(Double.parseDouble(token));
                    }
                    mCentroids.get(i).add(pointPosition);
                }
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] generateCentroidStringArray(int index) {
        ArrayList<String> strings = new ArrayList<>();
        for (int i = 0; i < mCentroids.get(index).size(); ++i) {
            strings.add(mCentroids.get(index).get(i).writeToString());
        }
        String[] result = new String[mCentroids.get(index).size()];
        result = strings.toArray(result);
        return result;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("usage: kmeans <data> <c1> <c2> <output>");
            return 1;
        }
        Cleaner.remove(args[3]);
        initializeCentroids(args[1], args[2]);
        Configuration configuration = getConf();
        Configuration jConf = new Configuration(configuration);
        jConf.setStrings("Centroids", generateCentroidStringArray(0));
        jConf.setInt("Norm", 1);
        Job job = Job.getInstance(jConf, "K-Means");
        job.setJarByClass(Main.class);
        job.setJobName("K Means");
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PointPosition.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(CentroidAssigner.CentroidMapper.class);
        job.setReducerClass(CentroidAssigner.CentroidReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.waitForCompletion(true);
        return 0;
    }
}
