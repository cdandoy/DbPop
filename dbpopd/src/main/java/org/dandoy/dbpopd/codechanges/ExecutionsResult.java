package org.dandoy.dbpopd.codechanges;

import org.dandoy.dbpop.database.ObjectIdentifier;

import java.util.List;

public record ExecutionsResult(List<Execution> fileExecutions, long executionTime) {
    public record Execution(ObjectIdentifier objectIdentifier, String name, String type, String error) {
        public Execution(ObjectIdentifier objectIdentifier, String error) {
            this(
                    objectIdentifier,
                    objectIdentifier.toQualifiedName(),
                    objectIdentifier.getType(),
                    error
            );
        }
    }
}
