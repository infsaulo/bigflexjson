package bigflexjson.grammar;

public enum SrcTypes {

  INTEGER, STRING, BYTES, DECIMAL;

  public static String[] names() {

    final SrcTypes[] types = SrcTypes.values();
    final String[] names = new String[types.length];

    for (int index = 0; index < types.length; index++) {

      names[index] = types[index].name();
    }

    return names;
  }
}
