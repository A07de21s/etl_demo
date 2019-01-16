package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.*;
// import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool
{
    public static LogGenericWritable parseLog(String row) throws Exception
    {
        String[] logPart = StringUtils.split(row, "\u1111");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = simpleDateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);

        LogGenericWritable logData = new LogWritable();
        logData.put("time_tag", new LogFieldWritable(timeTag));
        logData.put("active_name", new LogFieldWritable(activeName));
        for (Map.Entry<String, Object> entry : bizData.entrySet())
        {
            logData.put(entry.getKey(), new LogFieldWritable(entry.getValue()));
        }

        return logData;
    }

    public static class LogWritable extends LogGenericWritable
    {
        @Override
        protected String[] getFieldName() {
            return new String[] {"active_name", "session_id", "time_tag", "ip", "device_id", "req_url", "user_id", "product_id", "order_id", "error_flag", "error_log"};
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration configuration = getConf();
        configuration.addResource("mr.xml");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parseLog");
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setMapOutputKeyClass(TextLongWritable.class);
        job.setGroupingComparatorClass(TextLongGroupComparator.class);
        job.setPartitionerClass(TextLongPartitioner.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(LogOutputFormat.class);
        job.addCacheFile(new URI(configuration.get("ip.file.path")));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // FileOutputFormat.setCompressOutput(job, true);
        // FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath))
        {
            fileSystem.delete(outputPath,true);
        }

        if (!job.waitForCompletion(true))
        {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

        return 0;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, TextLongWritable, LogGenericWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            Counter errorCounter = context.getCounter("Log Error", "Parse Error");
            try
            {
                LogGenericWritable parseLog = parseLog(value.toString());
                String session = (String)parseLog.getObject("session_id");
                Long timeTag = (Long)parseLog.getObject("time_tag");

                TextLongWritable outKey = new TextLongWritable();
                outKey.setText(new Text(session));
                outKey.setCompareValue(new LongWritable(timeTag));

                context.write(outKey, parseLog);
            }
            catch (Exception e)
            {
                errorCounter.increment(1);
                LogGenericWritable v = new LogWritable();
                v.put("error_flag", new LogFieldWritable("error"));
                v.put("error_log", new LogFieldWritable(value));
                TextLongWritable outKey = new TextLongWritable();
                int randomKey = (int)(Math.random() * 100);
                outKey.setText(new Text("error" + randomKey));
                context.write(outKey, v);
            }
        }
    }

    public static class LogReducer extends Reducer<TextLongWritable, LogGenericWritable, Text, Text>
    {
        private JSONArray actionPath = new JSONArray();
        private Text sessionID;
        private StringBuilder stringBuilder = new StringBuilder();

        public void setup(Context context) throws IOException
        {
            FileReader fileReader = new FileReader("ipaddress.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String str = bufferedReader.readLine();
            stringBuilder.append(str);
        }

        public void reduce(TextLongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException
        {
            Text sid = key.getText();
            if(sessionID == null || !sid.equals(sessionID))
            {
                sessionID = new Text(sid);
                actionPath.clear();
            }

            for(LogGenericWritable v : values)
            {
                JSONObject datum = JSON.parseObject(v.asJSONString());
                if(v.getObject("error_flag") == null)
                {
                    String[] address = StringUtils.split(stringBuilder.toString(), ".");
                    JSONObject addr = new JSONObject();
                    addr.put("country", address[0]);
                    addr.put("province", address[1]);
                    addr.put("city", address[2]);

                    String activeName = (String)v.getObject("active_name");
                    String reqUrl = (String)v.getObject("req_url");
                    String pathUnit = "pageview".equals(activeName) ? reqUrl : activeName;
                    actionPath.add(pathUnit);


                    datum.put("address", addr);
                    datum.put("action_path", actionPath);
                }

                String outputKey = v.getObject("error_flag") == null ? "part" : "error/part";
                context.write(new Text(outputKey), new Text(datum.toJSONString()));
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
        System.exit(res);
    }
}
