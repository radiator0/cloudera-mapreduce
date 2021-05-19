import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;

public class Stage4Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs multipleOutputs;

    @Override
    protected void setup(Reducer.Context context) {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] tableAndKey = splitToTableAndKey(key);
        if(tableAndKey == null) {
            System.err.println("Malformed key: " + key.toString());
            System.err.println("For values: " + Util.join(values));
            return;
        }
        for (Text t : values) {
            multipleOutputs.write(tableAndKey[0], new Text(tableAndKey[1]), new Text(t.toString()), tableAndKey[0]);
            return;
        }
    }

    private String[] splitToTableAndKey(Text key) {
        String s = key.toString();
        String[] tables = new String[] { "fights", "fighters", "physiques", "positions", "results", "statistics" };
        for (String table: tables) {
            if(s.startsWith("_" + table)) {
                String rowKey = s.substring(table.length() + 2);
                return new String[] { table, rowKey };
            }
        }
        return null;
    }
}
