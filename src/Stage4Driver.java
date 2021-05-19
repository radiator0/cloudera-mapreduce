import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Stage4Driver {

    public static void main(String[] args) throws Exception {
        System.err.println("Start...");
        /*
         * Validate that two arguments were passed from the command line.
         */
        if (args.length != 2) {
            System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }

        /*
         * Instantiate a Job object for your job's configuration.
         */
        Job job = new Job();

        /*
         * Specify the jar file that contains your driver, mapper, and reducer.
         * Hadoop will transfer this jar file to nodes in your cluster running
         * mapper and reducer tasks.
         */
        job.setJarByClass(Stage4Driver.class);

        /*
         * Specify an easily-decipherable name for the job.
         * This job name will appear in reports and logs.
         */
        job.setJobName("Stage4 Driver");

        job.setMapperClass(Stage4Mapper.class);
        job.setReducerClass(Stage4Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        MultipleOutputs.addNamedOutput(job, "fighters", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "physiques", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "positions", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "results", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "fights", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "statistics", TextOutputFormat.class, Text.class, Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * Start the MapReduce job and wait for it to finish.
         * If it finishes successfully, return 0. If not, return 1.
         */
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

