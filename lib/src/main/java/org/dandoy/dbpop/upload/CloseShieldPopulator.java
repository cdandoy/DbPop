package org.dandoy.dbpop.upload;

import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.util.Map;

public class CloseShieldPopulator extends Populator {
    protected CloseShieldPopulator(ConnectionBuilder connectionBuilder, Database database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName) {
        super(connectionBuilder, database, datasetsByName, tablesByName);
    }

    @Override
    public void close() {
    }

    public void doClose() {
        super.close();
    }
}
