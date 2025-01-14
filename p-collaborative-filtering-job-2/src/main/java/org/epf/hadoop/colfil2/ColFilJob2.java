package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ColFilJob2 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputPath;
        String outputPath;
        
        if (args.length == 3 && args[0].contains("ColFilJob2")) {
            inputPath = args[1];
            outputPath = args[2];
        } else if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        } else {
            System.err.println("Usage: ColFilJob2 <input path> <output path>");
            System.err.println("Arguments reçus : " + args.length);
            for (int i = 0; i < args.length; i++) {
                System.err.println("Arg[" + i + "] : " + args[i]);
            }
            return -1;
        }

        System.out.println("Using input path: " + inputPath);
        System.out.println("Using output path: " + outputPath);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Job2: Common Friends Counter");
        job.setJarByClass(getClass());

        // Définir les formats d'entrée et de sortie
        job.setInputFormatClass(TextInputFormat.class);
        
        // Définir les classes de mapping et de reducing
        job.setMapperClass(CommonUsersMapper.class);
        job.setReducerClass(CommonUsersReducer.class);
        
        // Définir les types de sortie du Mapper
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Définir les types de sortie du Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);  // Changé pour Text au lieu de IntWritable
        
        // Définir le nombre de reducers
        job.setNumReduceTasks(2);

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Démarrage de ColFilJob2");
        int exitCode = ToolRunner.run(new Configuration(), new ColFilJob2(), args);
        System.exit(exitCode);
    }
}