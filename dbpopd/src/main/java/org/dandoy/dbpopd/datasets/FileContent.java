package org.dandoy.dbpopd.datasets;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record FileContent(long size, Integer rows) {}
