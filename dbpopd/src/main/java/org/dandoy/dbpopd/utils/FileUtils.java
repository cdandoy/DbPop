package org.dandoy.dbpopd.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {
    public static List<File> getFiles(File dir) {
        ArrayList<File> ret = new ArrayList<>();
        getFiles(dir, ret);
        return ret;
    }

    private static void getFiles(File dir, ArrayList<File> ret) {
        File[] files = dir.listFiles();
        if (files == null) {
            ret.add(dir);
        }else{
            for (File file : files) {
                getFiles(file, ret);
            }
        }
    }
}
