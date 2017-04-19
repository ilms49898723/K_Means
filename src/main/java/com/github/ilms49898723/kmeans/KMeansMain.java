package com.github.ilms49898723.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class KMeansMain extends Configured implements Tool {
    public static final int K = 2;
    public static final int MAX_ITER = 2;

    private ArrayList<PointPosition> mCentroid;

    private void initializeCentroids(String c) {
        try {
            mCentroid = new ArrayList<>();
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path path = new Path(c);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(path))
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
                mCentroid.add(pointPosition);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] generateCentroidStringArray() {
        ArrayList<String> strings = new ArrayList<>();
        for (int i = 0; i < mCentroid.size(); ++i) {
            strings.add(mCentroid.get(i).writeToString());
        }
        String[] result = new String[mCentroid.size()];
        result = strings.toArray(result);
        return result;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: kmeans <data> <c1> <c2>");
            return 1;
        }
        FileUtility.remove("centroids");
        FileUtility.mkdir("centroids");
        FileUtility.copyFile(args[1], "centroids/c1-L1");
        FileUtility.copyFile(args[1], "centroids/c1-L2");
        FileUtility.copyFile(args[2], "centroids/c2-L1");
        FileUtility.copyFile(args[2], "centroids/c2-L2");
        FileUtility.mkdir("costs");
        FileUtility.touch("costs/cost1-L1");
        FileUtility.touch("costs/cost1-L2");
        FileUtility.touch("costs/cost2-L1");
        FileUtility.touch("costs/cost2-L2");
        for (int i = 0; i < KMeansMain.MAX_ITER; ++i) {
            for (int centroidsIndex = 1; centroidsIndex <= 2; ++centroidsIndex) {
                for (int norm = 1; norm <= 2; ++norm) {
                    FileUtility.remove("output");
                    String centroidFilename = "centroids/c" + centroidsIndex + "-L" + norm;
                    String costFilename = "costs/cost" + centroidsIndex + "-L" + norm;
                    initializeCentroids(centroidFilename);
                    Configuration configuration = getConf();
                    Configuration jConf = new Configuration(configuration);
                    jConf.setStrings("Centroids", generateCentroidStringArray());
                    jConf.setInt("Norm", norm);
                    jConf.setInt("Iter", i + 1);
                    Job job = Job.getInstance(jConf, "K-Means");
                    job.setJarByClass(Main.class);
                    job.setMapOutputKeyClass(IntWritable.class);
                    job.setMapOutputValueClass(PointPosition.class);
                    job.setOutputKeyClass(NullWritable.class);
                    job.setOutputValueClass(Text.class);
                    job.setMapperClass(CentroidAssigner.CentroidMapper.class);
                    job.setReducerClass(CentroidAssigner.CentroidReducer.class);
                    job.setInputFormatClass(TextInputFormat.class);
                    job.setOutputValueClass(TextOutputFormat.class);
                    job.setNumReduceTasks(1);
                    FileInputFormat.addInputPath(job, new Path(args[0]));
                    FileOutputFormat.setOutputPath(job, new Path("output"));
                    MultipleOutputs.addNamedOutput(job, "cost", TextOutputFormat.class, NullWritable.class, Text.class);
                    job.waitForCompletion(true);
                    FileUtility.remove(centroidFilename);
                    FileUtility.copyFile("output/part-r-00000", centroidFilename);
                    FileUtility.append("output/cost-r-00000", costFilename);
                }
            }
        }
        return 0;
    }
}
