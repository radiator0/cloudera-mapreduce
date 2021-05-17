import org.apache.hadoop.io.Text;

public class Util {
    public static String Join(Object[] array) {
        if (array.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object el : array) {
                String s = el.toString().trim();
                sb.append(s).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        } else {
            return "";
        }
    }

    public static String Join(Iterable<Text> iterable) {
        StringBuilder sb = new StringBuilder();
        boolean didConcat = false;
        for (Text el : iterable) {
            String s = el.toString().trim();
            sb.append(s).append(",");
            didConcat = true;
        }
        if(didConcat) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}
