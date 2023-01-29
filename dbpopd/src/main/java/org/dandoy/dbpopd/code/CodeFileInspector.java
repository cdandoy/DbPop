package org.dandoy.dbpopd.code;

import java.io.File;

public class CodeFileInspector {

    /**
     * Walk the codeDirectory and invokes the visitor for each catalog, schema and file
     */
    public static void inspect(File codeDirectory, CodeFileVisitor visitor) {
        File[] catalogDirs = codeDirectory.listFiles();
        if (catalogDirs != null) {
            for (File catalogDir : catalogDirs) {
                String catalog = catalogDir.getName();
                visitor.catalog(catalog);
                File[] schemaFiles = catalogDir.listFiles();
                if (schemaFiles != null) {
                    for (File schemaFile : schemaFiles) {
                        String schema = schemaFile.getName();
                        visitor.schema(catalog, schema);
                        File[] codeTypeFiles = schemaFile.listFiles();
                        if (codeTypeFiles != null) {
                            for (File codeTypeFile : codeTypeFiles) {
                                String codeType = codeTypeFile.getName();
                                if (CodeService.CODE_TYPES.contains(codeType)) {
                                    File[] sqlFiles = codeTypeFile.listFiles();
                                    if (sqlFiles != null) {
                                        for (File sqlFile : sqlFiles) {
                                            String name = sqlFile.getName();
                                            if (sqlFile.isFile() && name.endsWith(".sql")) {
                                                visitor.module(catalog, schema, codeType, name, sqlFile);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public interface CodeFileVisitor {
        void catalog(String catalog);

        void schema(String catalog, String schema);

        void module(String catalog, String schema, String type, String name, File sqlFile);
    }
}
