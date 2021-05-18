import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class Stage1Test {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

        /*
         * Set up the mapper test harness.
         */
        Stage1Mapper mapper = new Stage1Mapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);

        /*
         * Set up the reducer test harness.
         */
        Stage1Reducer reducer = new Stage1Reducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);

        /*
         * Set up the mapper/reducer test harness.
         */
        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    /*
     * Test the mapper.
     */
    @Test
    public void testMapper() {

        /*
         * For this test, the mapper's input will be "1 cat cat dog"
         * TODO: implement
         */
        fail("Please implement test.");

    }

    /*
     * Test the reducer.
     */
    @Test
    public void testReducer() {

        /*
         * For this test, the reducer's input will be "cat 1 1".
         * The expected output is "cat 2".
         * TODO: implement
         */
        fail("Please implement test.");

    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() {

        /*
         * For this test, the mapper's input will be "1 cat cat dog"
         * The expected output (from the reducer) is "cat 2", "dog 1".
         * TODO: implement
         */
        fail("Please implement test.");

    }
}