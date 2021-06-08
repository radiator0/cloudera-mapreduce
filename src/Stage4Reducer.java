import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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

        String newKey = tableAndKey[1];
        for (Text t : values) {
            multipleOutputs.write(tableAndKey[0], new Text(newKey), new Text(t.toString()), tableAndKey[0] + "/" + tableAndKey[0]);
            return;
        }
    }

    private String[] splitToTableAndKey(Text key) {
        String s = key.toString();
        String[] tables = new String[] { "fights", "fighters", "physiques", "positions", "results", "statistics" };
        for (String table: tables) {
            if(s.startsWith("_" + table)) {
                String rowKey = s.substring(table.length() + 1);
                return new String[] { table, rowKey };
            }
        }
        return null;
    }
}
