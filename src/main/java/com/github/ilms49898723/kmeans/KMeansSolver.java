package com.github.ilms49898723.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;

public class KMeansSolver {
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, PointPosition> {
        private ArrayList<PointPosition> mCentroids;
        private int mNorm;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mCentroids = new ArrayList<>();
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
            ArrayList<Double> values = new ArrayList<>();
            for (String token : tokens) {
                values.add(Double.parseDouble(token));
            }
            PointPosition pointPosition = new PointPosition(values);
            int minIndex = 0;
            double minDis = pointPosition.distanceFrom(mCentroids.get(0), mNorm);
            for (int i = 1; i < KMeansMain.K; ++i) {
                double distance = pointPosition.distanceFrom(mCentroids.get(i), mNorm);
                if (distance < minDis) {
                    minDis = distance;
                    minIndex = i;
                }
            }
            IntWritable index = new IntWritable(minIndex);
            context.write(index, pointPosition);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, PointPosition, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mMultipleOutputs;
        private ArrayList<PointPosition> mCentroids;
        private int mNorm;
        private double mCost;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mMultipleOutputs = new MultipleOutputs<>(context);
            mCentroids = new ArrayList<>();
            mCost = 0.0;
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
        protected void reduce(IntWritable key, Iterable<PointPosition> values, Context context) throws IOException, InterruptedException {
            ArrayList<Double> centroid = new ArrayList<>();
            int size = 0;
            for (PointPosition pointPosition : values) {
                mCost += mCentroids.get(key.get()).distanceFrom(pointPosition, mNorm);
                for (int i = 0; i < pointPosition.size(); ++i) {
                    if (centroid.size() < i + 1) {
                        centroid.add(0.0);
                    }
                    centroid.set(i, centroid.get(i) + pointPosition.get(i));
                }
                ++size;
            }
            PointPosition pointPosition = new PointPosition();
            for (double value : centroid) {
                pointPosition.add(value / size);
            }
            Text output = new Text(pointPosition.toString());
            context.write(NullWritable.get(), output);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            int iter = context.getConfiguration().getInt("Iter", -1);
            String costString = iter + " " + mCost;
            mMultipleOutputs.write("cost", NullWritable.get(), new Text(costString));
            mMultipleOutputs.close();
        }
    }
}
