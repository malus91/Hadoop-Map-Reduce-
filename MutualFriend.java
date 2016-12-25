
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriend {
	
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String user1 = conf.get("user1Pair");
		String user2 = conf.get("user2Pair");		
		//Gets input line		
		String userFriends = value.toString();
		//Splits key and friendlist
		String[] userFriendData = userFriends.split("\t");		//Get key 		
		String userID = userFriendData[0];	
		
		Text keyEmit = new Text();	
		
		if(userFriendData.length==2)			
		{
			String userFriendList = userFriendData[1];				
			Text valueEmit = new Text();	
			valueEmit.set(userFriendList);
			StringTokenizer friendTokens = new StringTokenizer(userFriendData[1], ",");
			while(friendTokens.hasMoreTokens())
			{
				String userFriendID = friendTokens.nextToken();		
				if((user1.compareTo(userID)==0&&user2.compareTo(userFriendID)==0)||(user1.compareTo(userFriendID)==0&&user2.compareTo(userID)==0))
				{
				String newKey = buildMapEmitKey(userID,userFriendID);				
				keyEmit.set(newKey);
				///Emit userID and UserFriendID
				context.write(keyEmit,valueEmit);
				}
			}			
		}	
	}

	private String buildMapEmitKey(String userID, String userFriendID) {
		// TODO Auto-generated method stub
		if(userID.compareTo(userFriendID)<0)
			return userID+','+userFriendID;
		else
			return userFriendID+','+userID;	
		
	}
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
	public static HashMap<Text,Text> friendMap = new HashMap<Text,Text>();

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {		
		HashSet<String> rec = new HashSet<>();
		List<String> mutualFriends = new ArrayList<>();		
		String friends = values.iterator().next().toString();
		String[] ids = friends.split(",");
		for(String id:ids)
		{
			rec.add(id);
		}
		 
		if(values.iterator().hasNext())
		{
			friends =values.iterator().next().toString();
			ids = friends.split(",");
			for(String id:ids)
			{
				if(rec.contains(id))
				{
					mutualFriends.add(id);
				}
			}
		}
		
		if(mutualFriends.size()>0)
		{
			//String[] pairIDs = key.toString().split(",");			
			boolean commaFlg = false;
			//String outputKey = pairIDs[0]+","+pairIDs[1]+"\t";
			String outputStr = new String("");
			for(String mutualFrd:mutualFriends)
			{
				if(commaFlg)
				{
					outputStr +=",";
				}				
			     outputStr +=mutualFrd;
			     commaFlg = true;			     
			}
			//Text newKey = new Text(outputKey);			
			//context.write(newKey,new Text(outputStr));			
			context.write(key,new Text(outputStr));
		}
		else
		{
			context.write(key, new Text(""));
		}
		
	}
	
    }
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("user1Pair",otherArgs[2]);
		conf.set("user2Pair",otherArgs[3]);
		Job job = new Job(conf, "MutualFriendIdentification");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
