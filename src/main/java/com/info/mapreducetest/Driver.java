package com.info.mapreducetest; // This user defined package for the uniqeness and identification for application

import java.io.IOException; // This is used to handle the exception if the input output file is having any exception for file.
import org.apache.hadoop.fs.Path; // This class is used for converting string location to path.
import org.apache.hadoop.io.IntWritable; // This class is used for the output data type which is word count.
import org.apache.hadoop.io.Text; // This class is used for the output key data type whcih is word.
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // This class is used to set the file based input formate 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // this calss is used to set the file base output formate
import org.apache.hadoop.mapreduce.Job; // To create job for the analysis

public class Driver { //This is the driver class for the main entry point for the map and reduce. 
    
    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{ // Entry point method
	// IOException = We are doing the file operations to handle these exception we include this exception
	// InterruptedException = We are doning the multithrading in it so to handle the intrupted exceptions
	// ClassNotFoundException = We are giving the class for map and reduce so if that class not found to habdle this exception
        if(args.length<2){ // Accept only 2 cammand line input we have to add this condition
            System.out.println("Usage is [generic option] <input path> <output path>"); // give user friendly message if we enter only one or no parameters.
            System.exit(1); // if we forgot to five 2 or 1 file paths the programe will stops here.
        }
        Job job=new Job(); // create job object for the further analysis
        
        job.setJarByClass(Driver.class); //this sets the jar file where er can dind the map and reducer class
        job.setJobName("wordCount"); // Set the name to out job
        
        FileInputFormat.setInputPaths(job, new Path(args[0])); // This sets the file based input formate to the job with input file path.
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // This sets the file based output formate to the job with output file path.
        
        job.setMapperClass(MapperTest.class); // Here we set the mapper class to job object 
        job.setReducerClass(ReducerTest.class); // Here we set the reducer class to job object 
        
        job.setOutputKeyClass(Text.class); // Here we set the output key datatype which is word in our case
        job.setOutputValueClass(IntWritable.class); // Here we set the output values which is count in our case
        
        System.exit(job.waitForCompletion(true)?0:1); // here we check task is complited and finish the job
        
    }
}
