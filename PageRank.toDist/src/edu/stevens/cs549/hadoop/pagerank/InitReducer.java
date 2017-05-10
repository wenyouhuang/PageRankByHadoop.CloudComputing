package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;


public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
        /*
		 * Since default rank is 1, so we need only output node+rank and adjacency list
		 */
		boolean firstKey = true; // Used for adding the space delimiter
		StringBuilder sb = new StringBuilder();
		
		for (Text val : values) {
			if (firstKey == true) {
			sb.append(val.toString());
			firstKey = false;
			}
			else {
				sb.append(" " + val.toString().trim());
			}
		}
		
		context.write(new Text(key.toString() + "+1.0"), new Text(sb.toString()));

    }
}
