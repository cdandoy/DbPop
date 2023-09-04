package org.dandoy.dbpopd.deploy;

import org.dandoy.dbpop.database.ObjectIdentifier;

import java.util.Map;

public record GenerateFlywayScriptsResult(String filename, Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier) {}
