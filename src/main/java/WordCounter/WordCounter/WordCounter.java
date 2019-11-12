package WordCounter.WordCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Map;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;


public class WordCounter extends Configured implements Tool {
    
    public static class PathMapper extends Mapper<Object, Text, Object, Text> {
    	
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		StringTokenizer st = new StringTokenizer(value.toString());
            Text wordOut = new Text();
            FileSplit split = (FileSplit)context.getInputSplit();
            String path = split.getPath().toString();
            Text pathText = new Text();
            pathText.set(path);
            while (st.hasMoreTokens()){
                wordOut.set(st.nextToken());
                context.write(wordOut, pathText);
            }
    	}
    	
    }
    
    public static class PathReducer extends Reducer<Text, Text, Text, Text> {
    	
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		
    		
			HashMap<String,Integer> hashtable = new HashMap<String,Integer>();
		  	Iterator<Text> itr = values.iterator();
		    int count = 0;
		    String docID = new String();
			   
		    while (itr.hasNext()) {
	    		docID = itr.next().toString();
		 
			    if(hashtable.containsKey(docID)){
		    	    count = (hashtable.get(docID));
		    	    count += 1;
		    	    hashtable.put(docID, count);	
			    } else {
			    	hashtable.put(docID, 1);
			    }
		    }
		    
		    StringBuffer buf = new StringBuffer("");
			   for(Map.Entry<String, Integer> h: hashtable.entrySet())    
				buf.append(h.getKey() + ":" + h.getValue() + "\t");
      	   
    		Text output = new Text();
    		output.set(buf.toString());
    		context.write(key, output);
    		
    	}
    	
    }
    
    public class NotepadOutputFormat extends FileOutputFormat<Text, Text> {
    	
        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
           Path path = FileOutputFormat.getOutputPath(arg0);
           Path fullPath = new Path(path, "result.txt");
	       FileSystem fs = path.getFileSystem(arg0.getConfiguration());
	       FSDataOutputStream fileOut = fs.create(fullPath, arg0);
	       return new NotepadWriter(fileOut);
	    }
	  }
    
	  public class NotepadWriter extends RecordWriter<Text, Text> {
	      private DataOutputStream out;
	
	      public NotepadWriter(DataOutputStream stream) {
	          out = stream;
	          try {
	              out.writeBytes("results:\r\n");
	          }
	          catch (Exception ex) {
	          }  
	      }
	
	      @Override
	      public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
	          out.close();
	      }
	
	      @Override
	      public void write(Text arg0, Text arg1) throws IOException, InterruptedException {
	          out.writeBytes(arg0.toString() + ": " + arg1.toString());
	          out.writeBytes("\r\n");  
	      }
	  }
    

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 2)  {
            System.err.println("Usage: WordCount <input_file> <output_directory>");
            System.exit(2); // comunica ao Hadoop que aconteceu um erro (qualquer codigo diferente de 0 e erro)
        }
        
        System.setProperty("hadoop.home.dir", "/");
        

        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCounter.class);
        job.setMapperClass(PathMapper.class);
        job.setReducerClass(PathReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(10);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(NotepadOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/temp"));
        
        job.waitForCompletion(true);
        
        Configuration idConf = new Configuration();
        Job idJob = Job.getInstance(idConf);
        
        idJob.setJarByClass(WordCounter.class);
        idJob.setOutputKeyClass(Text.class);
        idJob.setNumReduceTasks(1);
        idJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(idJob, new Path(otherArgs[1] + "/temp"));
        FileOutputFormat.setOutputPath(idJob, new Path(otherArgs[0] + "/final"));

        return idJob.waitForCompletion(true) ? 0 : 1;
        
    }
    

    public static void main(String[] args) throws Exception {
     // TODO Auto-generated method stub
     ToolRunner.run(new Configuration(), new WordCounter(), args);
    }

}
