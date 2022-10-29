package org.dandoy.dbpopd;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

@MicronautTest
class DbpopdTest {

    @Inject
    DbpopdController dbpopdController;

    @Test
    void testItWorks() {
        List<String> dataset = List.of("base", "invoices", "invoice_details");
        DbpopdService.PopulateResult result = dbpopdController.populate(dataset);
        /*
            customers.csv         3
            invoice_details.csv   7
            invoices.csv          4
            products.csv          3
            total                 17
         */
        Assertions.assertEquals(17, result.rows());
    }
}
