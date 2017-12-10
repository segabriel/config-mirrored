package io.scalecube.config.utils;

import org.jasypt.util.text.BasicTextEncryptor;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EncryptionHandler implements Function<String, String> {
  private final static Pattern PATTERN = Pattern.compile("@ENC\\(.*?\\)");

  private final BasicTextEncryptor textEncryptor;

  public EncryptionHandler(String password) {
    textEncryptor = new BasicTextEncryptor();
    textEncryptor.setPassword(password);
  }

  @Override
  public String apply(String original) {
    String result = original;
    Matcher matcher = PATTERN.matcher(original);
    while (matcher.find()) {
      String group = matcher.group();
      String value = textEncryptor.decrypt(group.substring(5, group.length() - 1));
      result = original.substring(0, matcher.start()) + value + original.substring(matcher.end(), original.length());
    }
    return result;
  }

  public static void main(String[] args) {
    EncryptionHandler handler = new EncryptionHandler("qwerty");
    String target = handler.textEncryptor.encrypt("hello-world");
    System.out.println(target);
//    String original = "qwew q@ENC(" + target + ")we213123)wqeq weq";
    String original = "@ENC(" + target + ")we213123)wqeq weq";
//    String original = "qwew q@ENC(" + target + ")";
    System.out.println(original);
    String result = handler.apply(original);
    System.out.println(result);

  }
}
