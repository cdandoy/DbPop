package org.dandoy.dbpop.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.dandoy.dbpop.database.Query;

import java.util.List;

import static java.util.Collections.emptyList;

public record TableExecutionModel(String constraintName, List<Query> queries, List<TableExecutionModel> constraints) {
    @JsonCreator
    public TableExecutionModel(
            @JsonProperty("constraintName") String constraintName,
            @JsonProperty("queries") List<Query> queries,
            @JsonProperty("constraints") List<TableExecutionModel> constraints
    ) {
        this.constraintName = constraintName;
        this.queries = queries == null ? emptyList() : queries;
        this.constraints = constraints == null ? emptyList() : constraints;
    }

    public TableExecutionModel removeTableExecutionModel(String constraintName) {
        for (int i = constraints.size() - 1; i >= 0; i--) {
            TableExecutionModel constraint = constraints.get(i);
            if (constraintName.equals(constraint.constraintName())) {
                constraints.remove(i);
                return constraint;
            }
        }
        return null;
    }
}
