package org.dandoy.dbpopd.code;

import lombok.Getter;
import org.dandoy.dbpopd.utils.Pair;

import java.util.List;

@Getter
public class DownloadResult {
    private final String downloadedPath;
    private final List<Pair<String, Integer>> codeTypeCounts;
    private final long executionTime;

    public DownloadResult(String downloadedPath, List<Pair<String, Integer>> codeTypeCounts, long executionTime) {
        this.downloadedPath = downloadedPath;
        this.codeTypeCounts = codeTypeCounts;
        this.executionTime = executionTime;
    }

    public int getCodeTypeCount(String codeType) {
        for (Pair<String, Integer> codeTypeCount : codeTypeCounts) {
            if (codeType.equals(codeTypeCount.left())) {
                return codeTypeCount.right();
            }
        }
        return 0;
    }
}
