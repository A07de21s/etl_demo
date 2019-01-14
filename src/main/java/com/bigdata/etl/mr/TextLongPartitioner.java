package com.bigdata.etl.mr;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextLongPartitioner extends Partitioner<TextLongWritable, Writeable> {
    @Override
    public int getPartition(TextLongWritable textLongWritable, Writeable writeable, int numPartitions)
    {
        int hash = textLongWritable.getText().hashCode();
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }
}
