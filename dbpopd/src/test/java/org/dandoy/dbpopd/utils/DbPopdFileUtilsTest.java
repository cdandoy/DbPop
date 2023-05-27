package org.dandoy.dbpopd.utils;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.dandoy.dbpopd.utils.DbPopdFileUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DbPopdFileUtilsTest {
    @Test
    void testToFileName() {
        assertEquals("should_not_change", DbPopdFileUtils.encodeFileName("should_not_change"));
        test("a_b@c:d!");
        test("<name>.sql");
    }

    private static void test(String filename) {
        String encoded = encodeFileName(filename);
        String decoded = decodeFileName(encoded);
        assertEquals(filename, decoded);
    }

    @Test
    void testObjectIdentifier() {
        File codeDirectory = new File("/a/b/c");
        assertEquals(
                new ObjectIdentifier("USER_TABLE", "master", "advanced", "customer_types"),
                toObjectIdentifier(codeDirectory, new File(codeDirectory, "master/advanced/USER_TABLE/customer_types.sql"))
        );
        assertEquals(
                new ObjectIdentifier(
                        "FOREIGN_KEY_CONSTRAINT", "master", "advanced", "customers_customer_types_fk",
                        new ObjectIdentifier("USER_TABLE", "master", "advanced", "customers")
                ), toObjectIdentifier(codeDirectory, new File(codeDirectory, "master/advanced/FOREIGN_KEY_CONSTRAINT/customers/customers_customer_types_fk.sql")));

        assertNull(toObjectIdentifier(codeDirectory, new File(codeDirectory, "master/advanced/USER_TABLE/customer_types.txt")));
        assertNull(toObjectIdentifier(codeDirectory, new File(codeDirectory, "master/advanced/customer_types.sql")));
        assertNull(toObjectIdentifier(codeDirectory, new File(codeDirectory, "master/advanced/USER_TABLE/x/y/customer_types.sql")));
    }
}