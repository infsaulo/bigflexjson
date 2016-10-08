package bigflexjson.text.csv.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.StreamUtils;

import com.google.api.client.util.Charsets;
import com.wizzardo.tools.json.JsonTools;

import bigflexjson.grammar.Field;
import bigflexjson.grammar.Grammar;

public class CsvJsonReprCoder extends AtomicCoder<String> {

  private static final long serialVersionUID = 733277822855275829L;

  private final Grammar grammar;

  public CsvJsonReprCoder(final Grammar grammar) {

    this.grammar = grammar;
  }

  public CsvJsonReprCoder() {

    grammar = null;
  }

  @Override
  public void encode(final String value, final OutputStream outStream, final Context context)
      throws CoderException, IOException {

  }

  @Override
  public String decode(final InputStream inStream, final Context context)
      throws CoderException, IOException {

    final String[] parsedCsvEntry =
        new String(StreamUtils.getBytes(inStream), Charsets.UTF_8).split(",");

    final Map<String, Object> jsonMap = new HashMap<>();

    for (final Field field : grammar.getFields()) {

      switch (field.getDestType()) {
        case "INTEGER":
          jsonMap.put(field.getDestName(),
              Long.valueOf(parsedCsvEntry[Integer.valueOf(field.getName())]));
          break;
        case "STRING":
          jsonMap.put(field.getDestName(), parsedCsvEntry[Integer.valueOf(field.getName())]);
          break;
        case "DECIMAL":
          jsonMap.put(field.getDestName(),
              Double.valueOf(parsedCsvEntry[Integer.valueOf(field.getName())]));
          break;
        default:
          throw new IllegalStateException("not supported destType " + field.getDestType());
      }
    }

    return JsonTools.serialize(jsonMap);
  }

}
