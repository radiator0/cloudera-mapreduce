import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

public class Util {
    public static String joinElements(Object...array) {
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

    public static String join(String joiner, Object...array) {
        if (array.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object el : array) {
                String s = el.toString().trim();
                sb.append(s).append(joiner);
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        } else {
            return "";
        }
    }

    public static String joinIfEveryoneNonEmpty(Object...array) {
        if(nonNullOrEmpty(array)) {
            return joinElements(array);
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

    public static String[] split(Object o) {
        String s = o.toString();
        List<String> elements = new ArrayList<>();
        boolean quoted = false;
        int substringStart = 0;
        for (int i = 0; i < s.length(); i++) {
            if(s.charAt(i) == '"') {
                quoted = !quoted;
            } else if(s.charAt(i) == ',' && !quoted) {
                elements.add(s.substring(substringStart, i).trim());
                substringStart = i+1;
            }
        }
        elements.add(s.substring(substringStart).trim());
        return elements.toArray(new String[0]);
    }

    public static String[] toStringArray(Iterable<Text> objects) {
        List<String> elements = new ArrayList<>();
        for (Text t: objects) {
            elements.add(t.toString());
        }
        return elements.toArray(new String[0]);
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
