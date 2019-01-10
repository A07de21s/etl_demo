package com.bigdata.etl.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

abstract public class LogGenericWritable implements Writable {
    private LogFieldWritable[] datumArray;
    private String[] nameArray;
    private Map <String, Integer> nameIndex;

    abstract protected String[] getFieldName();

    public LogGenericWritable()
    {
        nameArray = getFieldName();
        if (nameArray == null)
        {
            throw new RuntimeException("The field names can not be null!");
        }

        nameIndex = new HashMap<String, Integer>();
        for (int i = 0; i < nameArray.length; i++)
        {
            if (nameIndex.containsKey(nameArray[i]))
            {
                throw new RuntimeException("The field " + nameArray[i] +" duplicate");
            }

            nameIndex.put(nameArray[i], i);
        }

        datumArray = new LogFieldWritable[nameArray.length];
        for (int i = 0; i < datumArray.length; i++)
        {
            datumArray[i] = new LogFieldWritable();
        }
    }

    public void put(String name, LogFieldWritable value)
    {
        int index = getIndexByName(name);
        datumArray[index] = value;
    }

    public LogFieldWritable getWritable(String name)
    {
        int index = getIndexByName(name);
        return datumArray[index];
    }

    public Object getObject(String name)
    {
        return getWritable(name).getObject();
    }

    private int getIndexByName(String name)
    {
        Integer index = nameIndex.get(name);
        if (index == null)
        {
            throw new RuntimeException("The field " + name + " not registered");
        }

        return index;
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, nameArray.length);
        for (int i = 0; i < nameArray.length; i++)
        {
            datumArray[i].write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        datumArray = new LogFieldWritable[length];
        for (int i = 0; i < length; i++)
        {
            LogFieldWritable value = new LogFieldWritable();
            value.readFields(in);
            datumArray[i] = value;
        }
    }

    public String asJSONString()
    {
        JSONObject json = new JSONObject();
        for (int i = 0; i < nameArray.length; i++)
        {
            json.put(nameArray[i], datumArray[i].getObject());
        }

        return json.toJSONString();
    }
}
