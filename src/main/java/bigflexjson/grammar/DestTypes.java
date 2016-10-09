package bigflexjson.grammar;

public enum DestTypes {

  STRING, BYTES, INTEGER, FLOAT, BOOLEAN, RECORD, TIMESTAMP;

  public static String[] names() {

    final DestTypes[] types = DestTypes.values();
    final String[] names = new String[types.length];

    for (int index = 0; index < types.length; index++) {

      names[index] = types[index].name();
    }

    return names;
  }
}
