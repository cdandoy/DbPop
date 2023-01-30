package org.dandoy.dbpopd.code;

import lombok.Getter;
import org.dandoy.dbpopd.utils.Pair;

import java.util.List;

@Getter
public class DownloadResult {
    private final List<Pair<String, Integer>> codeTypeCounts;
    private final long executionTime;

    public DownloadResult(List<Pair<String, Integer>> codeTypeCounts, long executionTime) {
        this.codeTypeCounts = codeTypeCounts;
        this.executionTime = executionTime;
    }
}
