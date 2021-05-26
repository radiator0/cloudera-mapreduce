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

public class Stage2Test {

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
        Stage2Mapper mapper = new Stage2Mapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);

        /*
         * Set up the reducer test harness.
         */
        Stage2Reducer reducer = new Stage2Reducer();
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
    public void testMapper() throws IOException {
        Text key = new Text("FIGHTER 1 vs FIGHTER 2 ref THE REFEREE");
        Text value = new Text("FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,KO/TKO,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0,5 of 11,15 of 32,1 of 2,2 of 2,0 of 0,0 of 0");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapDriver.run();

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,KO/TKO,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0,11,5,32,15,2,1,2,2,0,0,0,0";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
    }

    @Test
    public void testMapperWithNulls() throws IOException {
        Text key = new Text("FIGHTER 1 vs FIGHTER 2 ref THE REFEREE");
        Text value = new Text("FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,KO/TKO,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0,,,,,,");
        mapDriver.setInput(key, value);

        List<Pair<Text, Text>> result = mapDriver.run();

        assertEquals(1, result.size());
        String expectedK = "FIGHTER 1 vs FIGHTER 2 ref THE REFEREE";
        String expectedV = "FIGHTER 1,FIGHTER 2,2021-03-20,THE REFEREE,FIGHTER 1,Bantamweight,False,KO/TKO,Orthodox,Orthodox,170.18,165.1,177.8,170.18,135.0,135.0,,,,,,,,,,,,";
        assertEquals(expectedK, result.get(0).getFirst().toString());
        assertEquals(expectedV, result.get(0).getSecond().toString());
    }
}
