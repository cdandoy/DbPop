package org.dandoy.dbpop.download;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Where {
    private String column;
    private Object value;
}
