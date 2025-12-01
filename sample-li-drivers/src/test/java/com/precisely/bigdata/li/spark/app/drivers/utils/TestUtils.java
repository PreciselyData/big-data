/*
 * Copyright 2017, 2024 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
*/
package com.precisely.bigdata.li.spark.app.drivers.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TestUtils {
    public static List<String> getRowsSorted(File dir, boolean hasHeader) throws IOException {
        List<String> allRows = new ArrayList<>();
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().startsWith("part-")) {
                List<String> lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
                if (hasHeader && !lines.isEmpty()) {
                    lines.remove(0);
                }
                for (String row : lines) {
                    if (!row.trim().isEmpty()) {
                        allRows.add(row);
                    }
                }
            }
        }
        Collections.sort(allRows);
        return allRows;
    }

    public static boolean hasResultsFile(File directory) {
        return Objects.requireNonNull(directory.list((dir, name) -> name.startsWith("part-00000"))).length != 0;
    }

}
