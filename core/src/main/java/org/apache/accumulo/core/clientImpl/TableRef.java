package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.TTable;
import org.apache.accumulo.core.data.TableId;

// a reference to a table, after its ID has been resolved
public class TableRef {

  private final TableId id;
  private final String name;

  private TableRef(TableId tableId, String tableName) {
    this.id = requireNonNull(tableId);
    this.name = requireNonNull(tableName);
  }

  static TableRef resolve(ClientContext context, String tableName) throws TableNotFoundException {
    return new TableRef(context.getTableId(tableName), tableName);
  }

  TTable toThrift() {
    return new TTable(id.canonical(), name);
  }

  public TableId id() {
    return id;
  }

  public String name() {
    return name;
  }

}
