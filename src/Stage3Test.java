import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Stage3Test {

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
        Stage3Mapper mapper = new Stage3Mapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);

        /*
         * Set up the reducer test harness.
         */
        Stage3Reducer reducer = new Stage3Reducer();
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
     * Test the mapper WinLoser.
     */
    @Test
    public void testMapperWinLoser() throws IOException {
        LongWritable key = new LongWritable(0);
        Text value = new Text("FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0,11,5,32,15,2,1,2,2,0,0,0,0");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapDriver.run();

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,WINNER,LOSER,true,false,Bantamweight,False,,Orthodox,Orthodox,5.08,-5.08,177.8,170.18,21,22,11,5,32,15,2,1,2,2,0,0,0,0";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
    }

    /*
     * Test the mapper Draw.
     */
    @Test
    public void testMapperDraw() throws IOException {
        LongWritable key = new LongWritable(0);
        Text value = new Text("FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,DRAW,Bantamweight,False,,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0,11,5,32,15,2,1,2,2,0,0,0,0");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapDriver.run();

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,DRAW,DRAW,false,false,Bantamweight,False,,Orthodox,Orthodox,5.08,-5.08,177.8,170.18,21,22,11,5,32,15,2,1,2,2,0,0,0,0";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
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
