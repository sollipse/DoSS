/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name:
 * Partner 1 Login:
 *
 * Partner 2 Name:
 * Partner 2 Login:
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;
    
    public static final LongWritable NEGATIVETWO = new LongWritable(-2L);

    public static final LongWritable NEGATIVEONE = new LongWritable(-1L);

    public static class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() {
	    super(LongWritable.class);
	}
	
	public LongArrayWritable(LongWritable[] values) {
	    super(LongWritable.class, values);
	}
    }

    public static class NodeWritable implements Writable {
	public LongArrayWritable _neighbors;
	public MapWritable _distances;

	public NodeWritable(LongArrayWritable neighbors, MapWritable distances) {
	    _neighbors = neighbors;
	    _distances = distances;
	}

	public NodeWritable() {}

	public void write(DataOutput out) throws IOException {
	    _neighbors.write(out);
	    _distances.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    LongArrayWritable tempneighbors = new LongArrayWritable();
	    MapWritable tempdistances = new MapWritable();
	    tempneighbors.readFields(in);
	    tempdistances.readFields(in);
	    _neighbors = tempneighbors;
	    _distances = tempdistances;
	}
    }


    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class BFSMap extends Mapper<LongWritable, NodeWritable, 
				       LongWritable, NodeWritable> {

	public long depth;

        @Override
	    public void map(LongWritable key, NodeWritable value, Context context)
	    throws IOException, InterruptedException {
            
	    depth = Long.parseLong(context.getConfiguration().get("depth"));
	    //System.out.println("Current depth is " + depth);

	    Iterator<Writable> iterator = value._distances.keySet().iterator();
	    while(iterator.hasNext()){
		LongWritable source = (LongWritable) iterator.next();
		if (((LongWritable) value._distances.get(source)).get() == depth - 1L) {
		    LongWritable[] nullLong = new LongWritable[1];
		    nullLong[0] = new LongWritable(-1L);
		    LongArrayWritable nullNeighbor = new LongArrayWritable(nullLong);
		    MapWritable notification = new MapWritable();
		    notification.put(source, new LongWritable(depth));
		    for (Writable temp : value._neighbors.get()) {
			LongWritable neighbor = (LongWritable) temp;
			if (neighbor.get() >= 0L) {
			    //System.out.print("Depth " + depth + " Notifying " + neighbor.get());
			    context.write(neighbor, new NodeWritable(nullNeighbor, notification));
			}
		    }
		}
	    }
            context.write(key, value);
        }
    }

    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
					  LongWritable, LongWritable> {

        @Override
	    public void map(LongWritable key, LongWritable value, Context context)
	    throws IOException, InterruptedException {
	    context.write(key, value);
	    context.write(value, NEGATIVETWO);
        }
    }

    public static class OutputMap extends Mapper<LongWritable, NodeWritable,
					  LongWritable, LongWritable> {

        @Override
	    public void map(LongWritable key, NodeWritable value, Context context)
	    throws IOException, InterruptedException {
	    LongWritable out = new LongWritable(1L);
	    Iterator<Writable> iterator = value._distances.values().iterator();
	    while(iterator.hasNext()){
		LongWritable distance = (LongWritable) iterator.next();
		context.write(distance, out);
	    }
        }
    }



    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class BFSReduce extends Reducer<LongWritable, NodeWritable, 
					  LongWritable, NodeWritable> {

        public long denom;

	public long depth;

        public void reduce(LongWritable key, Iterable<NodeWritable> values, 
			   Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));
	    depth = Long.parseLong(context.getConfiguration().get("depth"));
	    System.out.println("Current depth is " + depth);

            // You can print it out by uncommenting the following line:
            //System.out.println(denom);

            // Example of iterating through an Iterable
            if (depth != 0) {
		MapWritable outdistances = new MapWritable();
		LongArrayWritable outneighbors = new LongArrayWritable();
		for (NodeWritable value : values) {
		    if (((LongWritable) value._neighbors.get()[0]).get() != -1L) {
			outneighbors = value._neighbors;
		    }
		    Iterator<Writable> iterator = value._distances.keySet().iterator();
		    while(iterator.hasNext()){
			LongWritable source = (LongWritable) iterator.next();
			if (!outdistances.containsKey(source) || ((LongWritable)outdistances.get(source)).get() > ((LongWritable) value._distances.get(source)).get()) {
			    System.out.println("Adding source " + source.get() + " with distance " + ((LongWritable) value._distances.get(source)).get());
			    outdistances.put(source, value._distances.get(source));
			}
		    }
		}
		context.write(key, new NodeWritable(outneighbors, outdistances));
	    } else {
		for (NodeWritable temp : values) {
		    if (Math.random() <= (1.0 / denom)) {
			temp._distances.put(key, new LongWritable(0));
		    }
		    context.write(key, temp);
		}
            }
        }

    }

    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
					     LongWritable, NodeWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
			   Context context) throws IOException, InterruptedException {
	    HashSet<LongWritable> output = new HashSet<LongWritable>();
	    int i = 0;
	    for (LongWritable value : values) {
		if (value.get() >= 0) {
		    output.add(new LongWritable(value.get()));
		}
	    }
	    LongWritable[] out;
	    if (output.size() > 0) { 
	        out = new LongWritable[output.size()];
		Iterator<LongWritable> iter = output.iterator();
		while (iter.hasNext()) {
		    LongWritable temp = iter.next();
		    out[i] = temp;
		    System.out.println(out[i].get());
		    i++;
		}
	    } else {
		out = new LongWritable[1];
		out[0] = NEGATIVETWO;
	    }
	    context.write(key, new NodeWritable(new LongArrayWritable(out), new MapWritable()));
	}
    }

    public static class OutputReduce extends Reducer<LongWritable, LongWritable,
					     LongWritable, LongWritable> {

	public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context) throws IOException, InterruptedException {
	    int sum = 0;
	    for (LongWritable value : values) {
		sum += value.get();
	    }
	    context.write(key, new LongWritable(sum));
	}
    }

    // ------- Add your additional Mappers and Reducers Here ------- //
















    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NodeWritable.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
	    conf.set("depth", (new Long(i)).toString());
	    job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(NodeWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NodeWritable.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(BFSMap.class); // currently the default Mapper
            job.setReducerClass(BFSReduce.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(OutputMap.class); // currently the default Mapper
        job.setReducerClass(OutputReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
