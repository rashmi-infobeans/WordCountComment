package com.info.mapreducetest; // This user defined package for the uniqeness and identification for application

import java.io.IOException; // This is used to handle the exception if the input output file is having any exception for file.
import org.apache.hadoop.io.IntWritable; // This class is used for the output data type which is int value for map.
import org.apache.hadoop.io.LongWritable;  // This class is used for the intermidiate output data type which is location for the each first word which is key for map.
import org.apache.hadoop.io.Text; // This class is used for the final output data type which text key for map.
import org.apache.hadoop.mapreduce.Mapper; // Package from where we have to extend our mapper bas class to custome mapper class

public class MapperTest extends Mapper<LongWritable,Text,Text,IntWritable>{ // extends the bas class with first split input(key,value) and output of map (Key,value) datatypes
    IntWritable one=new IntWritable(1);  // convert the primitive int data type value to the haddop wrapper data type
    
    @Override
    public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{ // over ride the map methode
	//input 1 key = it is key for intermidiage split which is byte location for the each row
	//input 2 Text = it is the value which is text all the lines.
	//input 3 Text = it is the final output which is text and here word.
	//input 4 IntWritable = it is the value for that word.
        
        String[] line=value.toString().split(" ");// First we split each row by using " " space delimiter and seperate all the words.
        for(String word:line){ // traverese the string array 
            context.write(new Text(word.toLowerCase()), one); // Write the map output in temp file withe key abd value here word and 1.
        }
        
    }
}
