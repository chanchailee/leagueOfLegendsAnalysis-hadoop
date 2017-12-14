/*
 * Author: Chanchai Lee
 * */
package minionkill;

import java.io.IOException;
import java.text.DecimalFormat;

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
 * The purpose of this class is to find:
 * Does number of minion kill correspond to team_win_rate for low MMR and high MMR?
 * 
 * This class will use input from MinionMapData.java output.
 * */

public class MinionMattersWinRateMMR {
	
	public static void main(String[] args) throws Exception {

		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		/* path for input file */
		Path input = new Path(files[0]);
		/* output directory for job1 */
		Path output = new Path(files[1]);

		Job job1 = new Job(c, "Find winner_team winrate that correspond to number of minion killed or not for both low and high MMR");
		/* Specific main class in side jar file */
		job1.setJarByClass(MinionMattersWinRateMMR.class);
		/* Specific name for mapper class */
		job1.setMapperClass(MMWRMMRMapper.class);
		/* Specific name for reducer class */
		job1.setReducerClass(MMWRMMRReducer.class);

		/* Modify number of reducers */
		int numReducers = 1;
		job1.setNumReduceTasks(numReducers);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output);

		System.exit(job1.waitForCompletion(true) ? 0 : 1);

	}
	
	
	public static class MMWRMMRMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k2 = new Text();
		Text v2 = new Text();

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String data = value.toString();
			String[] lines = data.split("\n");

			for (String line : lines) {

				String[] items = line.split(",");

				/*
				 * items[0] = minionKill_matters, items[1] = 1 or 0 , items[2] = mmr (low or high)
				 * 
				 */

					k2.set(items[2] + ",");
					v2.set(items[1].trim());

					/* write mapper output */
					con.write(k2, v2);
				
			}
		}
	}
	
	public static class MMWRMMRReducer extends Reducer<Text, Text, Text,Text> {

		Text v3 = new Text();
		int total_game = 0;
		int minionKillMatters_game =0;
		double minionKillMattersPerTotalGame =0;
		DecimalFormat numberFormat = new DecimalFormat("#.00");
		

		public void reduce(Text k2, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			total_game=0;
			minionKillMatters_game=0;
			minionKillMattersPerTotalGame=0;
			
			for (Text value : values) {
				
				
				total_game++;
				minionKillMatters_game+=Integer.parseInt(value.toString());
				
			}
			minionKillMattersPerTotalGame = (double)minionKillMatters_game/total_game;

			v3.set(numberFormat.format(minionKillMattersPerTotalGame)+","+"totalgame = "+total_game+" ,"
					+ "minionKillMatters_game= "+minionKillMatters_game);
			con.write(k2, v3);
		}
	}

}
