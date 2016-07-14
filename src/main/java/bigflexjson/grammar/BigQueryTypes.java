package bigflexjson.grammar;

public enum BigQueryTypes {

  STRING, BYTES, INTEGER, FLOAT, BOOLEAN, RECORD, TIMESTAMP;

  public static String[] names() {

    final BigQueryTypes[] types = BigQueryTypes.values();
    final String[] names = new String[types.length];

    for (int index = 0; index < types.length; index++) {

      names[index] = types[index].name();
    }

    return names;
  }
}
