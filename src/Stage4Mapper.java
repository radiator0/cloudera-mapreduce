import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class Stage4Mapper extends Mapper<Text, Text, Text, Text> {

    private MultipleOutputs multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitted = Util.split(value);
        if (splitted.length != 31) {
            System.err.println("Invalid row length: " + splitted.length);
            System.err.println(Util.join(splitted));
            return;
        }

        Object[] fight = new String[] { splitted[2], splitted[3], splitted[8], splitted[9], splitted[18] };
        Text fightKey = new Text(Util.join("-", fight));

        multipleOutputs.write("fights", fightKey, new Text(Util.join(fight)));

        addFighterRelevantData(splitted, 0, fightKey);
        addFighterRelevantData(splitted, 1, fightKey);
    }

    private void addFighterRelevantData(String[] splitted, int index, Text fightKey) throws IOException, InterruptedException {
        Text fighterKey = new Text(splitted[index]);

        // bmi, reach, height difference
        Object[] physique = new String[] { splitted[16 + index], splitted[14 + index], splitted[12 + index] };
        Text physiqueKey = new Text(Util.join("-", physique));
        Text positionKey = new Text(splitted[10 + index]);
        Text resultKey = new Text(splitted[4 + index]);
        Object[] statistic = new String[] {
                splitted[19 + index * 2], splitted[20 + index * 2],
                splitted[23 + index * 2], splitted[24 + index * 2],
                splitted[27 + index * 2], splitted[28 + index * 2],
                splitted[6 + index],
                fighterKey.toString(), physiqueKey.toString(), fightKey.toString(), resultKey.toString(), positionKey.toString()
        };
        Text statisticKey = new Text(Util.join("-", statistic));

        if(Util.nonNullOrEmpty(fighterKey, physiqueKey, positionKey, resultKey, statisticKey)) {
            multipleOutputs.write("fighters", fighterKey, fighterKey);
            multipleOutputs.write("physiques", physiqueKey, new Text(Util.join(physique)));
            multipleOutputs.write("positions", positionKey, positionKey);
            multipleOutputs.write("results", resultKey, resultKey);
            multipleOutputs.write("statistics", statisticKey, new Text(Util.join(statistic)));
        }
    }
}
