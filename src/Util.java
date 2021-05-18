import org.apache.hadoop.io.Text;

public class Util {
    public static String join(Object[] array) {
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

    public static String join(Iterable<Text> iterable) {
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

    public static String[] split(Object s) {
        String[] splitted = s.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for(int i = 0; i < splitted.length; i++) {
            splitted[i] = splitted[i].trim();
        }
        return splitted;
    }
}
