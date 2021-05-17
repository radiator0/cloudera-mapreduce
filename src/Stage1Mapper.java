import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Stage1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log logger = LogFactory.getLog(Stage1Mapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitted = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        logger.info("Row length: " + splitted.length);
        if(splitted.length == 145) {
            MapRajeevw(key, value, context, splitted);
        } else if (splitted.length == 46) {
            MapTheman(key, value, context, splitted);
        } else {
            logger.info("Invalid row length: " + splitted.length);
        }
    }

    private void MapRajeevw(LongWritable key, Text value, Context context, String[] splitted) throws IOException, InterruptedException {
        String winnerColor = splitted[5];
        String winner = null;
        String loser = null;
        String referee = splitted[2];
        if(winnerColor.equals("Draw")) {
            if(splitted[0].compareTo(splitted[1]) > 0) {
                winner = splitted[0];
                loser = splitted[1];
            } else {
                winner = splitted[1];
                loser = splitted[0];
            }
        } else {
            if(winnerColor.equals("Red")) {
                winner = splitted[0];
                loser = splitted[1];
            } else {
                winner = splitted[1];
                loser = splitted[0];
            }
        }
        String newKey = winner + " vs " + loser + " ref " + referee;
        String newValue = Util.Join(new String[] {
                winner, loser, referee, splitted[3], splitted[6], splitted[7], splitted[139], splitted[140], splitted[141], splitted[142]
        });
        context.write(new Text(newKey), new Text(newValue));
    }

    private void MapTheman(LongWritable key, Text value, Context context, String[] splitted) throws IOException, InterruptedException {
        String winner = splitted[2];
        String loser = splitted[1];
        String referee = splitted[8];

        String newKey = winner + " vs " + loser + " ref " + referee;
        String newValue = Util.Join(new String[] {
                winner, loser, referee
        });
        context.write(new Text(newKey), new Text(newValue));
    }
}
