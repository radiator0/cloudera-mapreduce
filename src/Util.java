import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Text;

public class Util {
    public static String join(Object...array) {
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

    public static String joinIfEveryoneNonEmpty(Object...array) {
        if(nonNullOrEmpty(array)) {
            return join(array);
        } else {
            return null;
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

    public static String[] toStringArray(Iterable<Text> objects) {
        Object[] arr = Iterables.toArray(objects, Text.class);
        String[] newArr = new String[arr.length];
        for(int i = 0; i < newArr.length; i++) {
            newArr[i] = arr[i].toString();
        }
        return newArr;
    }

    public static String repeat(String s, int n) {
        return new String(new char[n]).replace("\0", s);
    }

    public static boolean nonNullOrEmpty(Object...array) {
        for (Object el : array) {
            if(el == null || el.toString().trim().length() == 0) {
                return false;
            }
        }
        return true;
    }
}
