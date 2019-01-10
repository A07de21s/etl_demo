package com.bigdata.etl.mr;

import org.apache.hadoop.io.*;

public class LogFieldWritable extends GenericWritable {
    public LogFieldWritable()
    {
        set(NullWritable.get());
    }

    public LogFieldWritable(Object obj)
    {
        if (obj == null)
        {
            set(NullWritable.get());
        }
        else if (obj instanceof Writable)
        {
            set((Writable)obj);
        }
        else if (obj instanceof Long)
        {
            set(new LongWritable((Long)obj));
        }
        else if (obj instanceof Double)
        {
            set(new DoubleWritable((Double)obj));
        }
        else if (obj instanceof String)
        {
            set(new Text((String)obj));
        }
        else
        {
            throw new RuntimeException("The LogFieldWritable only access Long, Double, String and Writable type!");
        }
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return new Class[] {Text.class, LongWritable.class, NullWritable.class, DoubleWritable.class};
    }

    public Object getObject ()
    {
        Writable writable = get();
        if (writable instanceof Text)
        {
            return writable.toString();
        }
        else if (writable instanceof LongWritable)
        {
            return ((LongWritable)writable).get();
        }
        else if (writable instanceof DoubleWritable)
        {
            return ((DoubleWritable)writable).get();
        }
        else
        {
            return null;
        }
    }
}
