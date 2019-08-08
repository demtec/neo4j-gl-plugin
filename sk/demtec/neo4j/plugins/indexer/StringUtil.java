package sk.demtec.neo4j.plugins.indexer;

import java.text.Normalizer;
import java.text.Normalizer.Form;

public class StringUtil {
    public static String normalizeAndLowerCase(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return s;
        }
        return normalize(s).toLowerCase();
    }

    public static String normalize(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return s;
        }
        return Normalizer.normalize(s, Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    }
}
