import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class Stage3Mapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitted = Util.split(value);
        if (splitted.length != 28) {
            System.err.println("Invalid row length: " + splitted.length);
            System.err.println(Util.join(splitted));
        }

        String weightLbsA = splitted[14];
        String weightLbsB = splitted[15];

        String heightCmsA = splitted[10];
        String heightCmsB = splitted[11];

        String DRAW_TEXT = "DRAW", WINNER_TEXT = "WINNER", LOSER_TEXT = "LOSER";
        boolean isWinnerA = splitted[4].equals(splitted[0]);
        boolean isDraw = splitted[4].equals(DRAW_TEXT);

        List<String> output = new ArrayList<>();
        ArrayList<String> input = new ArrayList<>(Arrays.asList(splitted));

        output.addAll(input.subList(0, 4));
        output.addAll(
                Arrays.asList(
                        isDraw ? DRAW_TEXT : isWinnerA ? WINNER_TEXT : LOSER_TEXT,
                        isDraw ? DRAW_TEXT : isWinnerA ? LOSER_TEXT : WINNER_TEXT,
                        String.valueOf(!isDraw && isWinnerA),
                        String.valueOf(!isDraw && !isWinnerA)
                )
        );
        output.addAll(input.subList(5, 10));
        output.addAll(Arrays.asList(calculateHeightDiff(heightCmsA, heightCmsB), calculateHeightDiff(heightCmsB, heightCmsA)));
        output.addAll(input.subList(12, 14));
        output.addAll(Arrays.asList(calculateBmiAndRoundToInteger(heightCmsA, weightLbsA), calculateBmiAndRoundToInteger(heightCmsB, weightLbsB)));
        output.addAll(input.subList(16, 28));

        context.write(key, new Text(Util.join(output.toArray())));
    }

    private String calculateHeightDiff(String heightA, String heightB) {
        return String.format(Locale.US, "%.2f", Double.parseDouble(heightA) - Double.parseDouble(heightB));
    }

    private String calculateBmiAndRoundToInteger(String heightCms, String weightLbs) {
        return String.valueOf(calculateBmiAndRoundToInteger(Double.parseDouble(heightCms), Double.parseDouble(weightLbs)));
    }

    private int calculateBmiAndRoundToInteger(double heightCms, double weightLbs) {
        double LBS_TO_KG_RATIO = 0.45;
        return (int) Math.round(weightLbs * LBS_TO_KG_RATIO / Math.pow(heightCms / 100, 2));
    }

}
