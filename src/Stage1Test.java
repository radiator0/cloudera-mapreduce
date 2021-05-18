import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
        System.setProperty("hadoop.home.dir", "C:\\\\Hadoop");
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
    public void testMapperForR() throws IOException {
        final LongWritable key = new LongWritable(0);
        final Text value = new Text("FIGHTER 1,FIGHTER 2,THE REFEREE,2021-03-20,\"Las Vegas, Nevada, USA\",Red,False,Bantamweight,0.0,0.0,0.42,0.495,0.33,0.36,0.5,1.0,0.0,0.0,50.0,20.0,84.0,45.0,76.5,41.0,114.0,64.0,1.5,1.0,9.0,6.5,39.5,11.0,63.0,27.5,7.5,7.0,12.0,9.0,3.0,2.0,9.0,8.5,35.0,12.5,43.5,17.5,10.5,4.5,4.0,3.0,4.5,3.0,36.5,24.5,34.0,277.5,531.5,4,0,0,1,1,1,1,0,0,0,0,0,1,0,Orthodox,165.1,170.18,135.0,1.0,0.0,0.5,0.46,0.0,0.0,0.0,0.0,0.0,0.0,34.0,17.0,13.0,6.0,35.0,18.0,16.0,9.0,0.0,0.0,3.0,0.0,32.0,15.0,11.0,5.0,2.0,2.0,2.0,1.0,0.0,0.0,0.0,0.0,33.0,16.0,12.0,6.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,5.0,54.0,166.0,1,0,1,0,1,1,0,0,0,0,0,1,0,0,Orthodox,170.18,177.8,135.0,31.0,27.0");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapDriver.run();
        System.out.println(result);

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "1_FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
    }

    @Test
    public void testMapperForT() throws IOException {
        final LongWritable key = new LongWritable(0);
        final Text value = new Text("UFC Fight Night: Hall vs. Silva,FIGHTER 2 ,FIGHTER 1 ,Bantamweight Bout, KO/TKO ,1,2:46,3 Rnd (5-5-5),THE REFEREE,Kick to Head At Distance,FIGHTER1 ,1,17,34,50%,18 of 35,0,0,---,0,0,0:05,15 of 32,2 of 2,0 of 0,16 of 33,1 of 1,0 of 0,FIGHTER 2 ,0,6,13,46%,9 of 16,0,3,0%,0,0,0:54,5 of 11,1 of 2,0 of 0,6 of 12,0 of 1,0 of 0");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapDriver.run();
        System.out.println(result);

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "2_5 of 11,15 of 32,1 of 2,2 of 2,0 of 0,0 of 0";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
    }

    /*
     * Test the reducer.
     */
    @Test
    public void testReducer() throws IOException {
        final Text key = new Text( "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE");
        final Text value1 = new Text("2_5 of 11,15 of 32,1 of 2,2 of 2,0 of 0,0 of 0");
        final Text value2 = new Text("1_FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0");
        reduceDriver.setInput(key, Arrays.asList(value1, value2));

        List<Pair<Text, Text>> result = reduceDriver.run();
        System.out.println(result);

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0," +
                "5 of 11,15 of 32,1 of 2,2 of 2,0 of 0,0 of 0";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
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
