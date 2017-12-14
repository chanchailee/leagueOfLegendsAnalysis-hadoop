/*
 * author: Chanchai Lee
 * */
package bigdata.lol;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;

public class MMRGameCount {
	public static void main(String[] args) throws Exception {

		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		/* path for input file */
		Path input = new Path(files[0]);
		/* output directory for job1 */
		Path output = new Path(files[1]);

		Job job1 = new Job(c, "MMR GAME COUNT");
		/* Specific main class in side jar file */
		job1.setJarByClass(MMRGameCount.class);
		/* Specific name for mapper class */
		job1.setMapperClass(MGCMapper.class);
		/* Specific name for reducer class */
		job1.setReducerClass(MGCReducer.class);

		/* Modify number of reducers */
		// int numReducers = 0;
		// job1.setNumReduceTasks(numReducers);

		// job1.setNumReduceTasks(0);
		job1.setOutputKeyClass(Text.class);

		// job1.setOutputValueClass(Text.class);

		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output);

		System.exit(job1.waitForCompletion(true) ? 0 : 1);

	}

	public static class MGCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		Text k2 = new Text();
		IntWritable v2 = new IntWritable();
		int game_id = -1;
		double mmr = -1;

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

			/* Create objectMapper to store JSON object */

			String input = value.toString();
			JSONObject obj = new JSONObject(input);

			game_id = obj.getInt("game_id");
			mmr = obj.getDouble("mmr");

			if (game_id != -1 && mmr != -1) {
				k2.set(mmr + ",");

				v2.set(1);
				/* write the first mapreduce output */

				con.write(k2, v2);
			}

		}
	}

	public static class MGCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable v3 = new IntWritable();

		public void reduce(Text k2, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			v3.set(sum);
			con.write(k2, v3);
		}
	}

}
