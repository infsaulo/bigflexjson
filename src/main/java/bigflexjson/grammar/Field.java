package bigflexjson.grammar;

import java.io.Serializable;

public class Field implements Serializable {

  private static final long serialVersionUID = 769620709074629987L;

  // Original field's name
  String name;

  // Desirable end field's name
  String destName;

  // Type of data associated with the original field
  String srcType;

  // Desirable type of the end field
  String destType;

  // Determines the kind of serialization used in the original field.
  String srcSerialization;

  public String getName() {

    return name;
  }

  public String getDestName() {

    return destName;
  }

  public String getSrcType() {

    return srcType;
  }

  public String getDestType() {

    return destType;
  }

  public String getSrcSerialization() {

    return srcSerialization;
  }
}
