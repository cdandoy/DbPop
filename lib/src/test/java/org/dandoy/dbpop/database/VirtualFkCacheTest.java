package org.dandoy.dbpop.database;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.dandoy.DbPopUtils.invoiceDetails;
import static org.dandoy.DbPopUtils.invoices;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class VirtualFkCacheTest {
    @Test
    void name() throws IOException {
        File file = File.createTempFile("VirtualFkCache", ".json");
        if (!file.delete()) throw new RuntimeException();

        try {
            VirtualFkCache virtualFkCache = VirtualFkCache.createVirtualFkCache(file);
            virtualFkCache.addFK(
                    new ForeignKey(
                            "invoices_invoices_details_vfk",
                            null,
                            invoices,
                            List.of("invoice_id"),
                            invoiceDetails,
                            List.of("invoice_id")
                    )
            );
            List<ForeignKey> byPkTable = virtualFkCache.findByPkTable(invoices);
            assertEquals(1, byPkTable.size());
            assertEquals(invoices, byPkTable.get(0).getPkTableName());

            assertNotNull(virtualFkCache.getByPkTable(invoices, "invoices_invoices_details_vfk"));


            VirtualFkCache virtualFkCache2 = VirtualFkCache.createVirtualFkCache(file);
            assertNotNull(virtualFkCache2.getByPkTable(invoices, "invoices_invoices_details_vfk"));
        } finally {
            if (!file.delete()) //noinspection ThrowFromFinallyBlock
                throw new RuntimeException("Failed to delete " + file);
        }
    }
}