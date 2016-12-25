
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceJoin {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Gets input line
			String userFriends = value.toString();
			// Splits key and friendlist
			String[] userFriendData = userFriends.split("\t"); // Get key
			String userID = userFriendData[0];

			Text keyEmit = new Text();

			if (userFriendData.length == 2) {
				String userFriendList = userFriendData[1];
				userFriendList = userFriendList; // Indicates Friend List
				Text valueEmit = new Text();
				valueEmit.set(userFriendList);
				keyEmit.set(userID);
				/// Emit userID and UserFriendIDList
				context.write(keyEmit, valueEmit);

			}

		}

	}

	public static class userDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text userOutKey = new Text();
		private Text userOutData = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] userInfo = value.toString().split(",");
			if (userInfo.length == 10) {
				userOutKey.set(userInfo[0]); // UserID
				// Output Value string D +Address + City +State
				String data = userInfo[3] + ":" + userInfo[4] + ":" + userInfo[5]; // Indicates
																					// Data
																					// List
				userOutData.set(data);
				context.write(userOutKey, userOutData);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> friendList = new ArrayList<Text>();
		private ArrayList<Text> DataList = new ArrayList<Text>();
		private HashMap<String, Integer> friendAgeMap = new HashMap<>();

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String userDatafile = conf.get("userDataFile");
			Path userpath = new Path("hdfs://cshadoop1" + userDatafile);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(userpath)));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					String[] DOBParts = arr[9].toString().split("/");
					Date today = new Date();
					int currMonth = today.getMonth() + 1;
					int currYear = today.getYear() + 1900;
					Integer age = currYear - Integer.parseInt(DOBParts[2]);

					if (Integer.parseInt(DOBParts[0]) > currMonth)
						age--;
					else if (Integer.parseInt(DOBParts[0]) == currMonth) {
						// If month is same, but date of birth is large
						int currDay = today.getDate();
						if (Integer.parseInt(DOBParts[1]) > currDay)
							age--;
					}

					// Create the userMap for the friendID and Age
					friendAgeMap.put(arr[0].trim(), age);
				}
				line = br.readLine();
			}

		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			friendList.clear();
			DataList.clear();
			LongWritable keyEmit = new LongWritable();
			Text valueEmit = new Text();
			String dataList = new String("");
			String friendList = new String("");
			if (values.iterator().hasNext())
				friendList = values.iterator().next().toString();
			if (values.iterator().hasNext())
				dataList = values.iterator().next().toString();

			Integer maxAge = 0;
			Integer fAge = 0;
			
			// Get the friendList from one List
			// Text friendIDs = friendList.get(0);
			if (!friendList.equals("")){
				String[] friends = friendList.split(",");
				for (String fID : friends) {
					if (friendAgeMap.containsKey(fID)) {
						fAge = friendAgeMap.get(fID);
						if (maxAge < fAge)
						{
							maxAge = fAge;
							
						}
					}
				}
			}

				// Get the user Address info by Joining with the other Mapper
				// output

				// Text userData = DataList.get(0);
              if(!dataList.equals(""))
				{
				String[] details = dataList.split(":");
				String valueOut = new String("");
				for (String detail : details) {
					valueOut = valueOut + detail + ",";					
				}
				valueOut = valueOut + Integer.toString(maxAge);

				valueEmit.set(valueOut);

				if(maxAge!=0)
				context.write(key, valueEmit);
			}

		}
	}
	
	public static class FriendAge implements WritableComparable<FriendAge>{

		private Long friendID;
		private Integer age;		
		public FriendAge(){}
		
		public FriendAge(Long friendID, Integer age) {			
			this.friendID = friendID;
			this.age = age;
		}

		public Long getFriendID() {
			return this.friendID;
		}

		public void setFriendID(Long friendID) {
			this.friendID = friendID;
		}

		public Integer getAge() {
			return this.age;
		}

		public void setAge(Integer age) {
			this.age = age;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			friendID = in.readLong();
			age = in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(friendID);
			out.writeInt(age);
			
		}

		@Override
		public int compareTo(FriendAge compFriend) {
		
			int val = friendID.compareTo(compFriend.friendID);
			if(val !=0)
				return val;
			return this.age.compareTo(compFriend.age);
			
		}
		
		public String toString() {
			return friendID.toString() + ":" + age.toString();
			}
		
		@Override
		public boolean equals(Object obj) {
		    if (obj == null) {
		        return false;
		    }
		    if (getClass() != obj.getClass()) {
		        return false;
		    }
		    final FriendAge other = (FriendAge) obj;
		    if (this.friendID != other.friendID && (this.friendID== null || !this.friendID.equals(other.friendID))) {
		        return false;
		    }
		    if (this.age != other.age && (this.age == null || !this.age.equals(other.age))) {
		        return false;
		    }
		    return true;
		}
	}
	

	
	
	//Emit FriendID,AGE as the custom class and the userDetails
	public static class AgeMapper extends Mapper<LongWritable,Text,FriendAge,Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					
			String[] friendDetails = value.toString().split("\t");	
			//Text valueOut = new Text(value.toString().split("\t")[1]);
			
			if(friendDetails.length==2){
				String friendLine[] = friendDetails[1].split(",");			
			Long keyUSer = Long.parseLong(friendDetails[0]);		
			if(friendLine.length==4)
			{
			FriendAge keyOut = new FriendAge(keyUSer, Integer.parseInt(friendLine[3]));
			context.write(keyOut,new Text(friendDetails[1].toString()));
			//context.write(keyOut,valueOut);
			}
			}
		}
     }
	
	
	
	public class FriendAgePartitioner extends Partitioner<FriendAge,Text>{

		@Override
		public int getPartition(FriendAge friendClass, Text nullWritable, int partitionNum) {
	
			return friendClass.getAge().hashCode()%partitionNum;
		}
	    
	}
	
	public static class AgeSortComparator extends WritableComparator {

		public AgeSortComparator() {
			super(FriendAge.class,true);
			
		}		 
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			FriendAge key1 = (FriendAge) w1;
			FriendAge key2 = (FriendAge) w2;

			int cmpResult = -1*key1.getAge().compareTo(key2.getAge());
			
			return cmpResult;
		}
		
	}
	
	public static class AgeGroupingComparator extends WritableComparator {
		  public AgeGroupingComparator() {
				super(FriendAge.class, true);
			}

			@Override
    public int compare(WritableComparable w1, WritableComparable w2) {
				FriendAge key1 = (FriendAge) w1;
				FriendAge key2 = (FriendAge) w2;
				return -1*key1.getAge().compareTo(key2.getAge());
			}
		}
	
	public static class AgeReducer extends Reducer<FriendAge,Text,Text,Text>{
		TreeMap<String,String> finalOut = new TreeMap<>();

		@Override
		
		public void reduce(FriendAge keyIn, Iterable<Text> valueIn, Context context)
				throws IOException, InterruptedException {
			
			for(Text val:valueIn)
			{
				
				if(finalOut.size()<20)
				{
					finalOut.put(keyIn.getFriendID().toString(), val.toString());
					//context.write(new Text(Long.toString(keyIn.getFriendID())), new Text(val));
					context.write(new Text(keyIn.getFriendID().toString()), new Text(val));
				}
			}
		}
		
		
	}	

	public static void main(String[] args) throws Exception {

		Path outputInt1 = new Path(args[2]);

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("userDataFile", otherArgs[1]);
		if (otherArgs.length != 4) {
			System.err.println("Usage: ReduceJoin <input > <UserDataInput> <intermediate output> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "MaxAgeFriendReduceJOBChain");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(Reduce.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Map.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, userDetailsMapper.class);

		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job,outputInt1);
		
		int code = job.waitForCompletion(true)?0:1;
		
		Job job2 = new Job(new Configuration(), "SortAge");		
		job2.setJarByClass(ReduceJoin.class);
		FileInputFormat.addInputPath(job2,outputInt1);
		job2.setMapOutputKeyClass(FriendAge.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setPartitionerClass(FriendAgePartitioner.class);
		job2.setMapperClass(AgeMapper.class);
		job2.setSortComparatorClass(AgeSortComparator.class);
		job2.setGroupingComparatorClass(AgeGroupingComparator.class);
		job2.setReducerClass(AgeReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		// Wait till job completion
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
