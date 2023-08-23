package org.dandoy.dbpopd.code;

import org.dandoy.dbpopd.utils.Pair;

import java.util.List;

public record DownloadResult(String downloadedPath, List<Pair<String, Integer>> codeTypeCounts, long executionTime) {}
