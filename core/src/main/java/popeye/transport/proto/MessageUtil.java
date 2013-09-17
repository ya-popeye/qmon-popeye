package popeye.transport.proto;

/**
 * @author Andrey Stepachev
 */
public class MessageUtil {

  public static void validatePoint(final Message.Point point) {
    if (!point.hasIntValue() && !point.hasFloatValue())
      throw new IllegalArgumentException("Point doesn't have neither int nor float value: " + point);
    if (point.hasFloatValue()) {
      final float value = point.getFloatValue();
      if (Float.isNaN(value) || Float.isInfinite(value)) {
        throw new IllegalArgumentException("value is NaN or Infinite: " + value
                + " for " + point.toString());
      }
    }
    if ((point.getTimestamp() & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Illegal timestamp " + point.getTimestamp());
    }

    if (point.getAttributesCount() < 1)
      throw new IllegalArgumentException("Need at least 1 attribute"); // but why?
    validateString("metric name", point.getMetric());
    for (int i = 0; i < point.getAttributesCount(); i++) {
      final Message.Attribute attr  = point.getAttributes(i);
      validateString("attribute name", attr.getName());
      validateString("attribute value", attr.getValue());
    }
    validateString("metric name", point.getMetric());
  }

  public static void validateString(final String what, final String s) {
    if (s == null) {
      throw new IllegalArgumentException("Invalid " + what + ": null");
    }
    final int n = s.length();
    for (int i = 0; i < n; i++) {
      final char c = s.charAt(i);
      if (!(('a' <= c && c <= 'z')
              || ('A' <= c && c <= 'Z')
              || ('0' <= c && c <= '9')
              || c == '-' || c == '_' || c == '.' || c == '/')) {
        throw new IllegalArgumentException("Invalid " + what
                + " (\"" + s + "\"): illegal character: " + c);
      }
    }
  }
}
