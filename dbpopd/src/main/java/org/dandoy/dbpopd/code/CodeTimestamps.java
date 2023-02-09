package org.dandoy.dbpopd.code;

import java.sql.Timestamp;

/**
 * Timestamps stored in dbpop_timestamps
 */
public record CodeTimestamps(Timestamp fileTimestamp, Timestamp codeTimestamp) {}
