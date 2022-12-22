package org.dandoy.dbpop.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dandoy.dbpop.database.TableName;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
public class TableExecutionModel {
    private final TableName tableName;
    private List<Constraint> constraints = new ArrayList<>();

    @JsonCreator
    public TableExecutionModel(
            @JsonProperty("tableName") TableName tableName
    ) {
        this.tableName = tableName;
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    static class Constraint {
        private String name;
        private TableName parentTable;
        private TableName childTable;
    }
}
