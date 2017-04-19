package com.github.ilms49898723.kmeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PointPosition implements Writable, StringWritable {
    private ArrayList<Double> mValues;

    public PointPosition() {
        mValues = new ArrayList<>();
    }

    public PointPosition(String[] centroidSources) {
        for (String source : centroidSources) {
            mValues.add(Double.parseDouble(source));
        }
    }

    public PointPosition(ArrayList<Double> values) {
        mValues = new ArrayList<>();
        mValues.addAll(values);
    }

    public void add(double value) {
        mValues.add(value);
    }

    public double get(int index) {
        return mValues.get(index);
    }

    public ArrayList<Double> getAll() {
        return mValues;
    }

    public void set(int index, double value) {
        mValues.set(index, value);
    }

    public void setAll(ArrayList<Double> values) {
        mValues = new ArrayList<>();
        mValues.addAll(values);
    }

    public int size() {
        return mValues.size();
    }

    public double distanceFrom(PointPosition that, int norm) {
        double sum = 0.0;
        if (norm == 1) {
            int length = this.size();
            for (int i = 0; i < length; ++i) {
                double diff = this.get(i) - that.get(i);
                sum += Math.abs(diff);
            }
        } else if (norm == 2) {
            int length = this.size();
            for (int i = 0; i < length; ++i) {
                double diff = this.get(i) - that.get(i);
                sum += diff * diff;
            }
        } else {
            return -1;
        }
        return sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(mValues.size());
        for (double value : mValues) {
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            mValues.add(in.readDouble());
        }
    }

    @Override
    public String writeToString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(mValues.size());
        for (Double value : mValues) {
            stringBuilder.append(" ").append(value);
        }
        return stringBuilder.toString();
    }

    @Override
    public void restoreFromString(String source) {
        String[] tokens = source.split("\\s+");
        int size = Integer.parseInt(tokens[0]);
        mValues = new ArrayList<>();
        for (int i = 1; i <= size; ++i) {
            mValues.add(Double.parseDouble(tokens[i]));
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (double value : mValues) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(" ");
            }
            stringBuilder.append(value);
        }
        return stringBuilder.toString();
    }
}
