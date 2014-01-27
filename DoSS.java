/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name: Lucas Sloan
 * Partner 1 Login: cs61c-dk
 *
 * Partner 2 Name: Paul Kang
 * Partner 2 Login: cs61c-en
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
import org.apache.hadoop.io.BooleanWritable;
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
    
    /* NEGATIVE TWO LongWritable value to simplify casting and typing later.  */
    public static final LongWritable NEGATIVETWO = new LongWritable(-2L);

    public static final LongWritable NEGATIVEONE = new LongWritable(-1L);

    /** Wrapper for ArrayWritable class that allows us to convert arrays of
	LongWritables quickly to ArrayWritables **/
    public static class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() {
	    super(LongWritable.class);
	}
	
	public LongArrayWritable(LongWritable[] values) {
	    super(LongWritable.class, values);
	}
    }

    /** NodeWritable Class. Consists of 2 fields: arraywritable that contains neighbors and
	MapWritable that contains SOURCE Key and DISTANCE value pair.
    **/
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

    public static class Notification implements Writable {
	BooleanWritable _orginal;
	LongArrayWritable _neighbors;
	LongArrayWritable _distances;

	public Notification(LongArrayWritable neighbors, LongArrayWritable distances) {
	    _neighbors = neighbors;
	    _distances = distances;
	    _orginal = new BooleanWritable(true);
	}

	public Notification(LongArrayWritable distances) {
	    LongWritable[] nullLong = new LongWritable[1];
	    nullLong[0] = new LongWritable(-1L);
	    _neighbors = new LongArrayWritable(nullLong);
	    _distances = distances;
	    _orginal = new BooleanWritable(false);
	}

	public Notification() {}

	public void write(DataOutput out) throws IOException {
	    _orginal.write(out);
	    _neighbors.write(out);
	    _distances.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    BooleanWritable temporginal = new BooleanWritable();
	    LongArrayWritable tempneighbors = new LongArrayWritable();
	    LongArrayWritable tempdistances = new LongArrayWritable();
	    temporginal.readFields(in);
	    tempneighbors.readFields(in);
	    tempdistances.readFields(in);
	    _orginal = temporginal;
	    _neighbors = tempneighbors;
	    _distances = tempdistances;
	}
    }

    /* Second Mapper. Applies the Breadth-first search pseudocode and returns appropriate kvp
     of NODENAME and NODE ITSELF. */
    public static class BFSMap extends Mapper<LongWritable, NodeWritable, 
				       LongWritable, Notification> {

	public long depth;

        @Override
	    public void map(LongWritable key, NodeWritable value, Context context)
	    throws IOException, InterruptedException {
            
	    depth = Long.parseLong(context.getConfiguration().get("depth"));
	    //System.out.println("Current depth is " + depth);

	    Iterator<Writable> iterator = value._distances.keySet().iterator();
	    int size = value._distances.size();
	    int i = 0;
	    LongWritable[] output = new LongWritable[size * 2];
	    while(iterator.hasNext()){
		LongWritable source = (LongWritable) iterator.next();
		LongWritable distance = (LongWritable) value._distances.get(source);
		output[i] = source;
		output[i + 1] = distance;
		if (distance.get() == depth - 1L) {
		    LongWritable[] note = new LongWritable[2];
		    note[0] = source;
		    note[1] = new LongWritable(depth);
		    LongArrayWritable notification = new LongArrayWritable(note);
		    for (Writable temp : value._neighbors.get()) {
			LongWritable neighbor = (LongWritable) temp;
			if (neighbor.get() >= 0L) {
			    //System.out.print("Depth " + depth + " Notifying " + neighbor.get());
			    context.write(neighbor, new Notification(notification));
			}
		    }
		}
		i += 2;
	    }
	    
            context.write(key, new Notification(value._neighbors, new LongArrayWritable(output)));
        }
    }
    /** First map. Simple loader that takes list of edge pairs and constructs 
	neighbor-relations from them. NOTE: to account for isolated nodes, the value -2 is 
	written in as a neighbor, and in the event that a node has NO neighbors, a special
	case is assigned to context.write. **/
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
					  LongWritable, LongWritable> {

        @Override
	    public void map(LongWritable key, LongWritable value, Context context)
	    throws IOException, InterruptedException {
	    context.write(key, value);
	    context.write(value, NEGATIVETWO);
        }
    }
    /** Third map. Takes nodes from second mapper, retrieves different distances, then uses distances as keys
	to sum up values to. Essentially a histogram-maker.
    **/

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



    /* Second Reducer. Assigns the appropriate values to both source and distances. Sends the 
       new nodes, with collated distances to the third mapper to turn distance data into histograms.
     */
    public static class BFSReduce extends Reducer<LongWritable, Notification, 
					  LongWritable, NodeWritable> {

        public long denom;

	public long depth;

        public void reduce(LongWritable key, Iterable<Notification> values, 
			   Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));
	    depth = Long.parseLong(context.getConfiguration().get("depth"));
            // You can print it out by uncommenting the following line:
            //System.out.println(denom);

            // Example of iterating through an Iterable
            if (depth != 0) {
		MapWritable outdistances = new MapWritable();
		LongArrayWritable outneighbors = new LongArrayWritable();
		LongWritable curdepth = new LongWritable(depth);
		for (Notification value : values) {
		    if (value._orginal.get()) {
			outneighbors = value._neighbors;
			Writable[] neighs = value._distances.get();
			for (int i = 0 ; i < neighs.length ; i += 2) {
			    outdistances.put((LongWritable) neighs[i], (LongWritable) neighs[i + 1]);
			}
		    } else {
			Writable[] neighs = value._distances.get();
			if (!outdistances.containsKey((LongWritable) neighs[0])) {
			    outdistances.put((LongWritable) neighs[0], (LongWritable) neighs[1]);
			}
		    }
		}
		context.write(key, new NodeWritable(outneighbors, outdistances));
	    } else {
		for (Notification value : values) {
		    NodeWritable temp = new NodeWritable(value._neighbors, new MapWritable());
		    if (Math.random() <= (1.0 / denom)) {
			temp._distances.put(key, new LongWritable(0));
		    }
		    context.write(key, temp);
		}
            }
        }

    }

    /** First reducer. After taking a SOURCENODE and a NEIGHBORNODE from its mapper,
	it searches to make sure that -2 is not encountered, converts list of new neighbors
	to appropriate data type, then context.writes, creating a new NodeWritable.
     **/
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
		    i++;
		}
	    } else {
		out = new LongWritable[1];
		out[0] = NEGATIVETWO;
	    }
	    context.write(key, new NodeWritable(new LongArrayWritable(out), new MapWritable()));
	}
    }

    /** Third reducer. After recieving longwritable data from its mapper, it collates each distance by
	the number of its occurences. The result is a histogram of each distance.
    **/
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

	job.setNumReduceTasks(48);

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
	    job.setNumReduceTasks(48);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Notification.class);
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

	job.setNumReduceTasks(1);

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
