// Srujan Pothina : 800965600
package org.myorg;

import java.util.regex.*;
import java.io.*;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.File;
import java.lang.Math;
import java.util.Collection;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;

public class pageRank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(pageRank.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new pageRank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		// Temporary output files created and later deleted for N value calculation
		Path tOutputFile = new Path("outputT/part-r-00000");
		Path tOutput = new Path("outputT");

		FileSystem hadoopFS = FileSystem.get(conf);
		Job job = Job.getInstance(getConf(), "noOfPages");

		// Job instantiated to calculate the number of pages  
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path("outputT"));
		job.setMapperClass(Map0.class);
		job.setReducerClass(Reduce0.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		boolean res = job.waitForCompletion(true);

		String[] arr;
		String line = "";
		FileSystem file = FileSystem.get(getConf());
		// Reading number of pages from the temporary pages file created above
		BufferedReader reader = new BufferedReader(new InputStreamReader(file.open(tOutputFile)));

		double pageCount = 0.0;
		// Reading line to strip the page count calculated from first MR job
		if( (line = reader.readLine()) != null){
			arr = line.split("\\s+");
			pageCount = Double.parseDouble(arr[1].trim());
		}

		// Deleting the pageNum output file before going ahead to the Link Graph creation
		if (hadoopFS.exists(tOutput)) {
			hadoopFS.delete(tOutput, true);
		}

		if(res){
			// This is to set the total pages calculated from first map reduce and using via configuration
			conf.set("totalPageCount", Double.toString(pageCount));

			Job job1  = Job .getInstance(getConf(),"initialPageRank");
			job1.setJarByClass(this.getClass());
			// Settign input and output paths for Link Graph calculation
			FileInputFormat.addInputPaths(job1,args[0]); 
			FileOutputFormat.setOutputPath(job1,new Path(args[1]));

			job1.setMapperClass(Map1.class);
			job1.setReducerClass(Reduce1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setNumReduceTasks(1);
			res = job1.waitForCompletion(true);
		}

		// Paths for iterations which will be deleted after reading after every iteration
		if(res){
			int i=1;
			while(i<10){

				Job job2  = Job .getInstance(getConf(),"pgRankIterations");
				job2.setJarByClass(this.getClass());
				if(i == 1)
				{
					FileInputFormat.addInputPaths(job2, args[1]+"/part-r-00000");
				}
				else
				{
					FileInputFormat.addInputPaths(job2, args[1] + Integer.toString(i-1));
				}
				// Appending index value to input path
				FileOutputFormat.setOutputPath(job2,new Path(args[1] + i + "/"));

				job2.setMapperClass(Map2.class);
				job2.setReducerClass(Reduce2.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				job2.setNumReduceTasks(1);
				res = job2.waitForCompletion(true);

				if(i>1 && i<10){
					Path delete= new Path(args[1] + Integer.toString(i-1));
					hadoopFS.delete(delete, true);
				}
				i++;
			}
		}
		//Sorting pages based on their previous page rank output
		if(res)
		{
			Job job3  = Job .getInstance(getConf(), "pgRankSorting");
			job3.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job3, args[1]+"9");
			FileOutputFormat.setOutputPath(job3,  new Path(args[2]));
			job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			job3.setMapperClass( Map3.class);
			job3.setReducerClass( Reduce3.class);
			job3.setOutputKeyClass( DoubleWritable.class);
			job3.setOutputValueClass( Text.class);
			job3.setNumReduceTasks(1);
			res = job3.waitForCompletion(true);
		}
		if(res)
			return 0 ;
		else
			return 1;
	}

	/////////////////////////////////////////////////////////////////////////////
	// Class to count the number of pages in the corpus
	public static class Map0 extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable(1);
		private static final Pattern TITLE = Pattern .compile("<title>(.+?)</title>");
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String line  = lineText.toString();
			Text word  = new Text("pageTitleCount");
			Matcher matcher = TITLE.matcher(line); 
			String titlePage = "";
			//Fethcing lines
			while(matcher.find())
			{
				titlePage = matcher.group(1).trim();
			}
			if(!titlePage.isEmpty() && titlePage != null){

				context.write(word,one);
			}
		}
	}
	public static class Reduce0 extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > pageCounts,  Context context)
				throws IOException,  InterruptedException {
			Configuration conf = context.getConfiguration();
			int sumPages  = 0;
			for ( IntWritable count  : pageCounts) {
				sumPages  += count.get();
			}
			context.write( word,new IntWritable(sumPages) );
		}
	}

	////////////////////////////////////////////////////////////////

	public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  Text> {

		// Regular expressions for getting the title, pageLink and text tag containing text
		private static final Pattern TEXT = Pattern .compile("<text[^>]*>(.+?)</text>");
		private static final Pattern PAGE_LINK = Pattern .compile("\\[\\[(.+?)\\]\\]");
		private static final Pattern TITLE = Pattern .compile("<title>(.+?)</title>");
		// Mapper for colelcting outgoing links and mapping them to the incoming links to calculate page rank
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();

			// Get title of each page
			String pageTitle = "";
			Matcher matcher = TITLE.matcher(line);
			while (matcher.find()) {
				pageTitle = matcher.group(1).trim();
			}


			int noOfOutgoingLinks = 0;
			
			List<String> pages = new ArrayList<String>();
			
			// Links matcher to strip outgoing links from the title page
			// Checking if only page title exists
			if(pageTitle != null && !pageTitle.isEmpty()){

				matcher = TEXT.matcher(line);
				matcher.matches();
				
				// Stripping text from <text></text> tag
				String text = "";
				while(matcher.find())
				{
					text = matcher.group(1).trim();
				}
			// Stripping page link from the text stripped string
				matcher = PAGE_LINK.matcher(text);
				String print = "";
				while(matcher.find())
				{
					String temp = matcher.group(1).trim();
					// adding outgoing links to the list
					pages.add(temp);
					// Unique Delimiter as #$$$#
					print = print + temp + "#$$$#";
				}
				
				noOfOutgoingLinks = pages.size();

				double pgRank = 0.0;
				// writing to the output file only if a page has outlinks from it
				if(noOfOutgoingLinks > 0)
				{
					// Calculating page rank for individual outgoing links and writing it to the output
					pgRank = (double)1/(double)noOfOutgoingLinks;
					for (int i=0; i<noOfOutgoingLinks; i++){
						context.write(new Text(pages.get(i)), new Text(Double.toString(pgRank)));
					}
				}
				// Writing the delimiter seprated outlinks for each page to the output file
				context.write(new Text(pageTitle),new Text(print));
			}

		}
	}
	// Reducer for Link Graph generation and calculating page ran once
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text page, Iterable<Text> linksList, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			double totalPages = Double.parseDouble(conf.get("totalPageCount"));
			String outgoingLinks = "";
			double pgRank = 0.0;

			for(Text link: linksList){
				String  l = link.toString().trim();
				if(!l.isEmpty() && l !=null ){
					try{
						// total pages to be modified later
						pgRank+= (double)1/totalPages*Double.parseDouble(l);
					}
					catch(NumberFormatException ex){
						//We get all the outgoing links
						outgoingLinks = l;
					}
				}
			}
			// Dampanening factor being 0.85 we calculate the initial page rank of all pages
			String pageText = page.toString()+"#$$$#";
			pageText += Double.toString(0.15 + 0.85*(pgRank))+"#$$$$#"; 
			// writing each pages outgoing links and its respective page rank are written to file
			context.write(new Text(pageText), new Text(outgoingLinks));
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	// Map Reduce for calculating page rank iterations
	public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			double pgRank = 0.0;
			int n =0;
			String line  = lineText.toString();
			//We split key and value pair written by the previous Graph link reducer
			String[] linksTitle = line.split("#\\$\\$\\$\\$#"); 
			//Splitting titles where we used unique delimiter #$$$#
			String[] pgRankTitles = linksTitle[0].trim().split("#\\$\\$\\$#");
			// Output links split using delimiter #$$$#
			String[] linksList = linksTitle[1].trim().split("#\\$\\$\\$#"); 

			// Obtaining all the outlinks of the page
			int linksLength = linksList.length; 
			pgRank = Double.parseDouble(pgRankTitles[1].trim())/linksLength; 
			for ( String link  : linksList) {
				if(link != null && !link.isEmpty()){
					// Each pages indivual page rank with its respective outlinks is computed and sent to reducer to find the total page rank
					context.write(new Text(link), new Text(Double.toString(pgRank))); 
				}
			}
			context.write(new Text(pgRankTitles[0]),new Text(linksTitle[1])); 
		}
	}
	//Calculating total page rank for all pages by adding a pages incoming link pagerank values
	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text> links,  Context context)
				throws IOException,  InterruptedException {
			double pgRank = 0.0;
			String allLinks="";
			// Same routine to calculate the pagerank value for each page adding its list of outgoing pages
			for (Text link : links) {
				String  l = link.toString().trim();
				if(!l.isEmpty() && l !=null ){
					try{
						pgRank = pgRank + Double.parseDouble(l);
					}
					catch(NumberFormatException ex){
						allLinks = l;
					}
				}
			}
			// calculating page rank using dampening factor and multiplying it with the existing page rank we got from MR job 1
			String pageText = word.toString() + "#$$$#" + Double.toString(0.15 + 0.85*(pgRank)) + "#$$$$#";
			// Condition to check for dangling nodes and not print them to the output file
			if( allLinks != null && !allLinks.isEmpty() ){
				// Writing all iteration page rank value to the file
				context.write(new Text(pageText), new Text(allLinks));
			}
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	// We strip the title and its page rank value to the reducer to sort.
	public static class Map3 extends Mapper<LongWritable , Text , DoubleWritable , Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			double pgRank = 0.0;
			int n =0;
			String line  = lineText.toString();
			String[] linksTitle = line.split("#\\$\\$\\$\\$#"); 
			//Splitting titles where we used unique delimiter above
			String[] pgRankTitles = linksTitle[0].trim().split("#\\$\\$\\$#");

			pgRank = Double.parseDouble(pgRankTitles[1].trim());
			context.write(new DoubleWritable(pgRank),new Text(pgRankTitles[0].trim()));
		}
	}
	//Reducer will print sorted page title and its page rank values
	public static class Reduce3 extends Reducer<DoubleWritable ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( DoubleWritable pgRank,  Iterable<Text> pglinks,  Context context)
				throws IOException,  InterruptedException {
			// Routine to iterate each line and display page and its page rank value to the file
			for ( Text link  : pglinks) {
				context.write(link , new Text(pgRank.toString()));
			}
		}
	}
}