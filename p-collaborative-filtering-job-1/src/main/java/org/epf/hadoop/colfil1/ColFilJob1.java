package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ColFilJob1 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // Ignorer le premier argument s'il s'agit du nom de la classe
        String inputPath;
        String outputPath;
        
        if (args.length == 3 && args[0].contains("ColFilJob1")) {
            inputPath = args[1];
            outputPath = args[2];
        } else if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        } else {
            System.err.println("Usage: ColFilJob1 <input path> <output path>");
            System.err.println("Arguments received: " + args.length);
            for (int i = 0; i < args.length; i++) {
                System.err.println("Arg[" + i + "]: " + args[i]);
            }
            return -1;
        }

        System.out.println("Using input path: " + inputPath);
        System.out.println("Using output path: " + outputPath);

        Job job = Job.getInstance(getConf(), "Job1: User Relationships");
        job.setJarByClass(getClass());
        
        // Set custom input format
        job.setInputFormatClass(RelationshipInputFormat.class);
        
        // Set mapper and reducer classes
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);
        
        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set number of reducers
        job.setNumReduceTasks(2);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ColFilJob1(), args);
        System.exit(exitCode);
    }
}