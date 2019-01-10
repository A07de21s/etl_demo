package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.LogFieldWritable;
import com.bigdata.etl.mr.LogGenericWritable;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool
{
    public static LogGenericWritable parseLog(String row) throws ParseException
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
            return new String[] {"active_name", "session_id", "time_tag", "ip", "device_id", "req_url", "user_id", "product_id", "order_id"};
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parseLog");
        job.setMapperClass(LogMapper.class);
        // job.setNumReduceTasks(0);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputKeyClass(Text.class);

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

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogGenericWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            try
            {
                LogGenericWritable parseLog = parseLog(value.toString());
                context.write(key, parseLog);
            }
            catch (ParseException e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class LogReducer extends Reducer<LongWritable, LogGenericWritable, NullWritable, Text>
    {
        @Override
        public void reduce(LongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException
        {
            for (LogGenericWritable v : values)
            {
                context.write(null, new Text(v.asJSONString()));
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
        System.exit(res);
    }
}
