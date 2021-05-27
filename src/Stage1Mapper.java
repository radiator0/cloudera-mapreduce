import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Stage1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitted = Util.split(value);
        if(splitted.length == 144) {
            MapRajeevw(key, value, context, splitted);
        } else if (splitted.length == 46) {
            MapTheman(key, value, context, splitted);
        } else {
            System.err.println("Invalid row length: " + splitted.length);
            System.err.println(value);
        }
    }

    private void MapRajeevw(LongWritable key, Text value, Context context, String[] splitted) throws IOException, InterruptedException {
        if(splitted[0].equals("R_fighter")) {
            return;
        }
        String winnerColor = splitted[5];
        String winner = null;
        int winnerStatsStart = -1;
        String loser = null;
        int loserStatsStart = -1;
        String referee = splitted[2];
        boolean isDraw = winnerColor.equals("Draw");
        if(isDraw && splitted[0].compareTo(splitted[1]) > 0 || winnerColor.equals("Red")) {
            winner = splitted[0];
            winnerStatsStart = 138;
            loser = splitted[1];
            loserStatsStart = 71;
        } else {
            winner = splitted[1];
            winnerStatsStart = 71;
            loser = splitted[0];
            loserStatsStart = 138;
        }
        if(!Util.nonNullOrEmpty(winner, loser, referee)) return;
        String newKey = winner + " vs " + loser + " ref " + referee;
        String newValue = Util.joinIfEverythingNonEmpty(
                winner, loser, splitted[3], referee, isDraw ? "Draw" : winner, splitted[7], splitted[6],
                splitted[winnerStatsStart], splitted[loserStatsStart],
                splitted[winnerStatsStart + 1], splitted[loserStatsStart + 1],
                splitted[winnerStatsStart + 2], splitted[loserStatsStart + 2],
                splitted[winnerStatsStart + 3], splitted[loserStatsStart + 3]
        );
        if(newValue != null) {
            context.write(new Text(newKey), new Text("1_" + newValue));
        }
    }

    private void MapTheman(LongWritable key, Text value, Context context, String[] splitted) throws IOException, InterruptedException {
        if(splitted[0].equals("Title")) {
            return;
        }
        String winner = splitted[2];
        String loser = splitted[1];
        String referee = splitted[8];
        if(!Util.nonNullOrEmpty(winner, loser, referee)) return;

        int winnerStatsStart = -1;
        int loserStatsStart = -1;

        if(winner.equals(splitted[10])) {
            winnerStatsStart = 22;
            loserStatsStart = 40;
        } else {
            winnerStatsStart = 40;
            loserStatsStart = 22;
        }

        String newKey = winner + " vs " + loser + " ref " + referee;
        String newValue = Util.joinElements(
                splitted[4],
                splitted[winnerStatsStart], splitted[loserStatsStart],
                splitted[winnerStatsStart + 1], splitted[loserStatsStart + 1],
                splitted[winnerStatsStart + 2], splitted[loserStatsStart + 2]
        );
        context.write(new Text(newKey), new Text("2_" + newValue));
    }
}
