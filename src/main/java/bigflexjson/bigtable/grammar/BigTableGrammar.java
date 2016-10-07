package bigflexjson.bigtable.grammar;

import java.io.Serializable;
import java.util.List;

public class BigTableGrammar implements Serializable {

  private static final long serialVersionUID = 3871222943056524700L;

  // List of Fields
  List<BigTableField> fields;

  public List<BigTableField> getFields() {

    return fields;
  }

}
