package com.cloudwick.pig.load;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bijay on 1/4/15.
 * custom load function to load the log data to pig.
 */
public class LogLoad extends LoadFunc {
  protected LineRecordReader in = null;
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  public static final Pattern PATTERN = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[(.*)\\] \"([^\\s]+)" +
          " (/[^\\s]*) HTTP/[^\\s]+\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"$");
  Matcher matcher;
  private ArrayList<Object> mProtoTuple = null;

  public LogLoad() {
  }


  @Override
  public InputFormat getInputFormat() {
    return new TextInputFormat();
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    in = (LineRecordReader) reader;
  }


  @Override
  public void setLocation(String location, Job job)
          throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public Tuple getNext() throws IOException {
    matcher = PATTERN.matcher("");
    String line;

    while (in.nextKeyValue()) {
      Text val = in.getCurrentValue();
      line = val.toString();
      if (line.length() > 0 && line.charAt(line.length() - 1) == '\r') {
        line = line.substring(0, line.length() - 1);
      }
      matcher = matcher.reset(line);
      ArrayList list = new ArrayList();
      if (matcher.find()) {
        for (int i = 1; i <= matcher.groupCount(); i++) {
          list.add(new DataByteArray(matcher.group(i)));
        }
        return mTupleFactory.newTuple(list);
      }
    }
    return null;
  }
}

