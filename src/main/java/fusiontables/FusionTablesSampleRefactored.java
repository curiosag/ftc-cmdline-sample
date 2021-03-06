package fusiontables;

import com.google.api.client.util.DateTime;
import com.google.common.base.Optional;

import cg.common.core.SystemLogger;
import structures.TableInfo;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class FusionTablesSampleRefactored {

  private static FusionTablesConnector connector;

  public static void main(String[] args) {
    try {
      Optional<AuthInfo> noAuth = Optional.absent(); 
      connector = new FusionTablesConnector(new SystemLogger(), noAuth);
     
      listTables();
      
      String tableId = connector.createSampleTable();

      System.out.println("using table " + tableId);

      insertData(tableId);
      showRows(tableId);
      connector.deleteTable(tableId);

      return;
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.exit(1);
  }

  /**
   * @param tableId
   * @throws IOException
   */
  private static void showRows(String tableId) throws IOException {
    View.header("Showing Rows From Table");

    String sql = "SELECT Text,Number,Location,Date FROM " + tableId;

    try {
      String response = connector.executeSql(sql);

      System.out.println(response);

    } catch (IllegalArgumentException e) {
    }
  }

  private static void listTables() {
    View.header("Listing My Tables");

    List<TableInfo> tablelist = connector.getTableInfo();

    if (tablelist.isEmpty()) {
      System.out.println("No tables found!");
      return;
    }

    for (TableInfo table : tablelist) {
      View.show(table);
      View.separator();
    }
  }
  
  

  private static void insertData(String tableId) throws IOException {
    String sql =
        "INSERT INTO " + tableId + " (Text,Number,Location,Date) " + "VALUES (" + "'Google Inc', "
            + "1, " + "'1600 Amphitheatre Parkway Mountain View, " + "CA 94043, USA','"
            + new DateTime(new Date()) + "')";

    try {
      String response = connector.executeSql(sql);
      System.out.println(response);

    } catch (IllegalArgumentException e) {
    }
  }

}
