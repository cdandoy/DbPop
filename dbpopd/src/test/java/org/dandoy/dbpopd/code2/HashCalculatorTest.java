package org.dandoy.dbpopd.code2;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseProxy;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HashCalculatorTest {
    private Collection<ObjectIdentifier> exclude(Collection<ObjectIdentifier> a, Collection<ObjectIdentifier> b) {
        Set<ObjectIdentifier> ret = new HashSet<>(a);
        ret.removeAll(b);
        return ret;
    }

    private Collection<ObjectIdentifier> intersect(Collection<ObjectIdentifier> a, Collection<ObjectIdentifier> b) {
        Set<ObjectIdentifier> ret = new HashSet<>(a);
        ret.retainAll(b);
        return ret;
    }

    @Test
    void name() {
        File codeDirectory = new File("D:\\git\\dbpop\\ws1\\files\\config\\code");
        UrlConnectionBuilder connectionBuilder = new UrlConnectionBuilder("jdbc:sqlserver://localhost:2433;database=tempdb;trustServerCertificate=true", "sa", "GlobalTense1010");
        try (DatabaseProxy database = Database.createDatabase(connectionBuilder)) {
            Map<ObjectIdentifier, ObjectSignature> fileSignatures = HashCalculator.captureSignatures(codeDirectory);
            Map<ObjectIdentifier, ObjectSignature> databaseSignatures = HashCalculator.captureSignatures(database);
            exclude(fileSignatures.keySet(), databaseSignatures.keySet()).forEach(objectIdentifier -> {
                System.out.println("Missing db");
            });
            exclude(databaseSignatures.keySet(), fileSignatures.keySet()).forEach(objectIdentifier -> {
                System.out.println("Missing file");
            });
            intersect(fileSignatures.keySet(), databaseSignatures.keySet()).forEach(objectIdentifier -> {
                ObjectSignature fileSignature = fileSignatures.get(objectIdentifier);
                ObjectSignature databaseSignature = databaseSignatures.get(objectIdentifier);
                if (!Arrays.equals(fileSignature.hash(), databaseSignature.hash())) {
                    System.out.println("Different signatures: " + objectIdentifier);
                }
            });
        }
    }

    @Test
    void testCleanStoreProcSql() {
        String expected = HashCalculator.cleanSql("""
                -- Does something something\r
                CREATE PROCEDURE GetInvoices @invoice_id INT AS\r
                BEGIN\r
                    SELECT invoice_id, customer_id, invoice_date\r
                    FROM invoices\r
                    WHERE invoice_id = @invoice_id\r
                END\r
                """);

        assertEquals(expected, HashCalculator.cleanCreateOrReplaceSql("""
                -- Does something something
                CREATE   PROC GetInvoices @invoice_id INT AS
                BEGIN
                    SELECT invoice_id, customer_id, invoice_date
                    FROM invoices
                    WHERE invoice_id = @invoice_id
                END
                
                
                """));
    }
}