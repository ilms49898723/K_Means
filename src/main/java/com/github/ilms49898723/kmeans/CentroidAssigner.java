package com.github.ilms49898723.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CentroidAssigner {
    public static class CentroidMapper extends Mapper<Object, Text, IntWritable, PointPosition> {
        private ArrayList<PointPosition> mCentroids;
        private double mCost;
        private int mNorm;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mCentroids = new ArrayList<>();
            mCost = 0;
            Configuration conf = context.getConfiguration();
            mNorm = conf.getInt("Norm", -1);
            String[] centroidSources = conf.getStrings("Centroids");
            for (String source : centroidSources) {
                PointPosition pointPosition = new PointPosition();
                pointPosition.restoreFromString(source);
                mCentroids.add(pointPosition);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().isEmpty()) {
                return;
            }
            String[] tokens = value.toString().split("\\s+");
            PointPosition pointPosition = new PointPosition(tokens);
            int minIndex = 0;
            double minDis = pointPosition.distanceFrom(mCentroids.get(0), mNorm);
            for (int i = 1; i < KMeansMain.K; ++i) {
                double distance = pointPosition.distanceFrom(mCentroids.get(i), mNorm);
                if (distance < minDis) {
                    minDis = distance;
                    minIndex = i;
                }
            }
            mCost += minDis;
            IntWritable index = new IntWritable(minIndex);
            context.write(index, pointPosition);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            context.getConfiguration().setDouble("Cost", mCost);
        }
    }

    public static class CentroidReducer extends Reducer<IntWritable, PointPosition, NullWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<PointPosition> values, Context context) throws IOException, InterruptedException {
            ArrayList<Double> centroid = new ArrayList<>();
            for (int i = 0; i < KMeansMain.K; ++i) {
                centroid.add(0.0);
            }
            int size = 0;
            for (PointPosition pointPosition : values) {
                for (int i = 0; i < pointPosition.size(); ++i) {
                    centroid.set(i, centroid.get(i) + pointPosition.get(i));
                }
                ++size;
            }
            PointPosition pointPosition = new PointPosition();
            for (double value : centroid) {
                pointPosition.add(value / size);
            }
            Text output = new Text(pointPosition.writeToString());
            context.write(NullWritable.get(), output);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
