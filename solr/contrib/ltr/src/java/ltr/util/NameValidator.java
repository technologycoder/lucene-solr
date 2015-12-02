package ltr.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameValidator {
  private static Pattern pattern = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_.\\-/\\(/\\)]*$");

  public static boolean check(String name) {
    if (name == null) {
      return false;
    }
    Matcher matcher = pattern.matcher(name);
    return matcher.find();
  }

}
