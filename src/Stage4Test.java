import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

public class Stage4Test {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    MapDriver<Text, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {
        System.setProperty("hadoop.home.dir", "C:\\\\Hadoop");
        /*
         * Set up the mapper test harness.
         */
        Stage4Mapper mapper = new Stage4Mapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);

        /*
         * Set up the reducer test harness.
         */
        Stage4Reducer reducer = new Stage4Reducer();
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
    public void testMapper() throws IOException {
        Text key = new Text("Aaron Riley vs Jorge Gurgel ref Mario Yamasaki");
        Text value = new Text("Aaron Riley,Jorge Gurgel,2008-11-15,Mario Yamasaki,WINNER,LOSER,true,false,Lightweight,False,Southpaw,Orthodox,2.54,-2.54,175.26,175.26,23,24,Decision - Unanimous,172,63,166,46,27,21,26,21,23,14,7,3");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapReduceDriver.run();
        System.out.println(result);
        //assertEquals(1, result.size());
        //String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        //String expectedV = "FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,WINNER,LOSER,true,false,Bantamweight,False,Orthodox,Orthodox,5.08,-5.08,177.8,170.18,21,22,KO/TKO,11,5,32,15,2,1,2,2,0,0,0,0";
       // assertEquals(expectedK, result.get(0).getFirst().toString());
        //assertEquals(expectedV, result.get(0).getSecond().toString());
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
