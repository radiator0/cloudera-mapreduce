import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Stage2Mapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitted = Util.split(value);
        if(splitted.length != 22) {
            System.err.println("Invalid row length: " + splitted.length);
            System.err.println(splitted);
        }

        String[] head_a = splitted[16].split(" ");
        String[] head_b = splitted[17].split(" ");
        String[] body_a = splitted[18].split(" ");
        String[] body_b = splitted[19].split(" ");
        String[] leg_a = splitted[20].split(" ");
        String[] leg_b = splitted[21].split(" ");

        String head_attempts_a = head_a.length == 3 ? head_a[2] : "";
        String head_landed_a = head_a.length == 3 ? head_a[0] : "";
        String head_attempts_b = head_b.length == 3 ? head_b[2] : "";
        String head_landed_b = head_b.length == 3 ? head_b[0] : "";

        String body_attempts_a = body_a.length == 3 ? body_a[2] : "";
        String body_landed_a = body_a.length == 3 ? body_a[0] : "";
        String body_attempts_b = body_b.length == 3 ? body_b[2] : "";
        String body_landed_b = body_b.length == 3 ? body_b[0] : "";

        String leg_attempts_a = leg_a.length == 3 ? leg_a[2] : "";
        String leg_landed_a = leg_a.length == 3 ? leg_a[0]: "";
        String leg_attempts_b = leg_b.length == 3 ? leg_b[2] : "";
        String leg_landed_b = leg_b.length == 3 ? leg_b[0] : "";

        ArrayList<String> newValueArrayList = new ArrayList<>(Arrays.asList(splitted));
        List<String> cutList = newValueArrayList.subList(0, 16);

        cutList.add(head_attempts_a);
        cutList.add(head_landed_a);
        cutList.add(head_attempts_b);
        cutList.add(head_landed_b);

        cutList.add(body_attempts_a);
        cutList.add(body_landed_a);
        cutList.add(body_attempts_b);
        cutList.add(body_landed_b);

        cutList.add(leg_attempts_a);
        cutList.add(leg_landed_a);
        cutList.add(leg_attempts_b);
        cutList.add(leg_landed_b);

        String newValue = Util.join(cutList.toArray());
        context.write(key, new Text(newValue));
    }
}
