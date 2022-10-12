package org.dandoy.dbpop.upload;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DataFileHeader {
    private final String columnName;
    private final boolean binary;
    private boolean loadable = true;

    public DataFileHeader(String header) {
        boolean binary = false;
        while (true) {
            int i = header.lastIndexOf('*');
            if (i == -1) break;
            String opt = header.substring(i + 1);
            if ("b64".equals(opt)) binary = true;
            header = header.substring(0, i);
        }
        this.binary = binary;
        this.columnName = header;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isBinary() {
        return binary;
    }
}
