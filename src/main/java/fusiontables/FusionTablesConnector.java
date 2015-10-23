package fusiontables;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.fusiontables.Fusiontables;
import com.google.api.services.fusiontables.Fusiontables.Query.Sql;
import com.google.api.services.fusiontables.Fusiontables.Table.Delete;
import com.google.api.services.fusiontables.FusiontablesScopes;
import com.google.api.services.fusiontables.model.Column;
import com.google.api.services.fusiontables.model.Sqlresponse;
import com.google.api.services.fusiontables.model.Table;

import cg.common.check.Check;
import cg.common.core.Logging;
import interfeces.Connector;
import interfeces.TableInfo;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FusionTablesConnector implements Connector {

  private final Map<String, String> tableNamesToIds = new HashMap<String, String>();

  private final String APPLICATION_NAME = "FutC";

  private FileDataStoreFactory dataStoreFactory;
  private HttpTransport httpTransport;
  private final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private Fusiontables fusiontables;

  private Logging logger;

  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".store/fusion_tables_sample");

  public FusionTablesConnector(Logging logger) {
    Check.notNull(logger);
    
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
      Credential credential = authorize();

      fusiontables = new Fusiontables.Builder(httpTransport, JSON_FACTORY, credential)
          .setApplicationName(APPLICATION_NAME).build();

      getTableInfo();

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    this.logger = logger;
  }

  private void log(String msg) {
    if (logger != null) logger.Info(msg);
  }

  private Credential authorize() throws Exception {
    String path = "/client_secrets.json";

    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
        new InputStreamReader(FusionTablesSample.class.getResourceAsStream(path)));

    if (clientSecrets.getDetails().getClientId().startsWith("Enter")
        || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
      throw new RuntimeException(
          "Enter Client ID and Secret from https://code.google.com/apis/console/?api=fusiontables "
              + "into fusiontables-cmdline-sample/src/main/resources/client_secrets.json");
    }

    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport,
        JSON_FACTORY, clientSecrets, Collections.singleton(FusiontablesScopes.FUSIONTABLES))
            .setDataStoreFactory(dataStoreFactory).build();

    return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
  }

  @Override
  public List<TableInfo> getTableInfo() {
    ArrayList<TableInfo> result = new ArrayList<TableInfo>();

    try {
      for (Table t : fusiontables.table().list().execute().getItems())
        result.add(new TableInfo(t.getName(), t.getTableId(), t.getDescription()));

    } catch (IOException e) {
      log(e.getMessage());
    }

    String fuckedUp = "";
    tableNamesToIds.clear();
    for (TableInfo i : result) {
      if (tableNamesToIds.containsKey(i.name))
        fuckedUp = fuckedUp + "ambiguous table name '" + i.name + "'\r\n";
      tableNamesToIds.put(i.name, i.id);

      
    }
    if (fuckedUp.length() > 0) {
      log( "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\r\n"
          + " ambiguous table names found (one name for more than one table ID)."
          + " Name to ID replacement has a good chance to produce \r\n"
          + " invalid queries if those tables are involved. \r\n"
          + " Better to change the name, a separate one per table. \r\n"
          + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\r\n" + fuckedUp);
    }
    
    return result;
  }

  @Override
  public Map<String, String> getTableNameToIdMap() {
    return tableNamesToIds;
  }

  @Override
  public String executeSql(String query) throws IOException {

    Sql sql = fusiontables.query().sql(query);
    Sqlresponse response = null;

    try {
      response = sql.execute();
    } catch (IllegalArgumentException e) {
      // For google-api-services-fusiontables-v1-rev1-1.7.2-beta this exception will always
      // been thrown.
      // Please see issue 545: JSON response could not be deserialized to Sqlresponse.class
      // http://code.google.com/p/google-api-java-client/issues/detail?id=545
    }
    getTableInfo();
    return response.toPrettyString();
  }

  @Override
  public String execSql(String query) {
    try {
      String result = executeSql(query);
      getTableInfo();
      return result;
    } catch (IOException e) {
      return e.getMessage();
    }
  }

  @Override
  public void deleteTable(String tableId) throws IOException {
    Delete delete = fusiontables.table().delete(tableId);
    delete.execute();
    getTableInfo();
  }

  public String createSampleTable() throws IOException {
    View.header("Create Sample Table");

    Table table = new Table();
    table.setName(UUID.randomUUID().toString());
    table.setIsExportable(false);
    table.setDescription("Sample Table");

    table.setColumns(Arrays.asList(new Column().setName("Text").setType("STRING"),
        new Column().setName("Number").setType("NUMBER"),
        new Column().setName("Location").setType("LOCATION"),
        new Column().setName("Date").setType("DATETIME")));

    Fusiontables.Table.Insert t = fusiontables.table().insert(table);
    Table r = t.execute();

    return r.getTableId();
  }

  @Override
  public String renameTable(String tableId, String newName) {

    Table table = new Table();
    table.setTableId(tableId);
    table.setName(newName);
    try {
      fusiontables.table().patch(tableId, table).execute();
    } catch (IOException e) {
      return e.getMessage();
    }

    getTableInfo();

    return "renamed " + tableId + " to " + newName;
  }
}
