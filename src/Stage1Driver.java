import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Paths;

public class Stage1Driver {

    public static void main(String[] args) throws Exception {
        System.err.println("Start...");
        /*
         * Validate that two arguments were passed from the command line.
         */
        if (args.length != 3) {
            System.out.printf("Usage: StubDriver <input dir> <input dir> <output dir>\n");
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
        job.setJarByClass(Stage1Driver.class);

        /*
         * Specify an easily-decipherable name for the job.
         * This job name will appear in reports and logs.
         */
        job.setJobName("Stage1 Driver");

        job.setMapperClass(Stage1Mapper.class);
        job.setReducerClass(Stage1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        File directory1 = new File(Paths.get(args[0]).getParent().toString());
        File directory2 = new File(Paths.get(args[1]).getParent().toString());

        final String fileName1 = Paths.get(args[0]).getFileName().toString();
        final String fileName2 = Paths.get(args[1]).getFileName().toString();

        File inputFile1 = directory1.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.startsWith(fileName1);
            }
        })[0];

        File inputFile2 = directory2.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.startsWith(fileName2);
            }
        })[0];


        FileInputFormat.setInputPaths(job, new Path(inputFile1.getPath()), new Path(inputFile2.getPath()));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        /*
         * Start the MapReduce job and wait for it to finish.
         * If it finishes successfully, return 0. If not, return 1.
         */
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

