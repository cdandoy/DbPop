package org.dandoy.dbpop.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
public class TableExecutionModel {
    private final String constraintName;
    private final List<TableExecutionModel> constraints;

    @JsonCreator
    public TableExecutionModel(
            @JsonProperty("constraintName") String constraintName,
            @JsonProperty("constraints") List<TableExecutionModel> constraints
    ) {
        this.constraintName = constraintName;
        this.constraints = constraints == null ? Collections.emptyList() : constraints;
    }

    public TableExecutionModel removeTableExecutionModel(String constraintName) {
        for (int i = constraints.size() - 1; i >= 0; i--) {
            TableExecutionModel constraint = constraints.get(i);
            if (constraintName.equals(constraint.getConstraintName())) {
                constraints.remove(i);
                return constraint;
            }
        }
        return null;
    }
}
