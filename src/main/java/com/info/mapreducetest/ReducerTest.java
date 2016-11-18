package com.info.mapreducetest; // This user defined package for the uniqeness and identification for application

import java.io.IOException; // This is used to handle the exception if the input output file is having any exception for file.
import org.apache.hadoop.io.IntWritable; // This class is used for the output data type which is word count.
import org.apache.hadoop.io.Text; // This class is used for the output key data type whcih is word.
import org.apache.hadoop.mapreduce.Reducer; // Package from where we have to extend bas class to our custome reducer class

public class ReducerTest extends Reducer<Text,IntWritable,Text,IntWritable>{ // Extends the base class Reducer and define the data type for input <Key,value> and output <Key,value>
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{// overide the abstract method 
	// Map -> shuffel and sort is input to reducer 
	// Input1 key = text key which is word 
	//input 2 = the value clunt implimets the iterable interface to iterate the values
	// input 3 = its message queue or pipeline to write the output in output file.
        int count=0; // Initiolization of the count
        for(IntWritable one:values){ // iterate the values for key
            count=one.get()+count; // get the sum or total of all the values
        }
        context.write(key, new IntWritable(count)); // write the count for key that is word in output file
    }
    
}
