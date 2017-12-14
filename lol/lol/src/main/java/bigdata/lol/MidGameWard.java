/*
 * Author: Chanchai Lee
 * */
package bigdata.lol;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/*
 * Purpose: Find ward position from total games for Mid game interval 
 * 
 * This class uses input from MapWard.java's output
 * 
 * input format:  game_id, mmr , match_result, x, y,interval
 * 
 * 
 * output:
 * 	KEY: interval = mid
 * 	Value : x,y	
 * */

public class MidGameWard {
	
	public static void main(String[] args) throws Exception {

		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		/* path for input file */
		Path input = new Path(files[0]);
		/* output directory for job1 */
		Path output = new Path(files[1]);

		Job job1 = new Job(c, "Find Mid Game Ward");
		/* Specific main class in side jar file */
		job1.setJarByClass(MidGameWard.class);
		/* Specific name for mapper class */
		job1.setMapperClass(MGWMapper.class);
		/* Specific name for reducer class */
		job1.setReducerClass(MGWReducer.class);

		/* Modify number of reducers */
		int numReducers = 1;
		job1.setNumReduceTasks(numReducers);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output);

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class MGWMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k2 = new Text();
		Text v2 = new Text();

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String data = value.toString();
			String[] lines = data.split("\n");

			for (String line : lines) {

				String[] items = line.split(",");

				/*
				 * items[0] = game_id , items[1] = mmr , items[2] = match_result
				 * items[3] = x ,items[4] = y ,items[5] = interval
				 */

				if (items[5].trim().equals("mid")) {

					k2.set("mid" + ",");
					v2.set(items[3].trim() + "," + items[4]);

					/* write mapper output */
					con.write(k2, v2);

				}

			}
		}
	}
	
	public static class MGWReducer extends Reducer<Text, Text, Text, Text> {

		Text v3 = new Text();

		public void reduce(Text k2, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			for (Text value : values) {
				v3.set(value);
				con.write(k2, v3);
			}

		}
	}

}
