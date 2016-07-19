package bigflexjson.grammar;

import java.io.Serializable;
import java.util.List;

public class Grammar implements Serializable {

  private static final long serialVersionUID = 6399559430588231251L;

  // List of Fields
  List<Field> fields;

  public List<Field> getFields() {

    return fields;
  }
}
