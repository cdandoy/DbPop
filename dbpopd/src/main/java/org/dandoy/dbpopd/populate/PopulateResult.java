package org.dandoy.dbpopd.populate;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record PopulateResult(int rows, long millis) {}
