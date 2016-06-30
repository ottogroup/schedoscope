package org.schedoscope.metascope.util;

public class ParseUtil {

  public static Integer tryParse(String text) {
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

}
