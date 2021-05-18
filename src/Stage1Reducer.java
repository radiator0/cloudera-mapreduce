import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class Stage1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] v = Util.toStringArray(values);
        if(v.length > 2) return;
        Arrays.sort(v);
        if(v[0].startsWith("2_") || v.length > 1 && !v[1].startsWith("2_")) return;
        String first = v[0].substring(2);
        String second = null;
        if(v.length > 1) {
            second = v[1].substring(2);
        } else {
            second = Util.repeat(",", 7);
        }

        context.write(key, new Text(first + "," +  second));
    }
}
