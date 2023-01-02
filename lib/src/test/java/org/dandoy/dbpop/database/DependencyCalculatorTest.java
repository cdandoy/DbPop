package org.dandoy.dbpop.database;

import org.dandoy.LocalCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DependencyCalculatorTest {
    private static final TableName invoices = new TableName("master", "advanced", "invoices");
    private static final TableName invoiceDetails = new TableName("master", "advanced", "invoice_details");

    @BeforeAll
    static void beforeAll() {
        LocalCredentials
                .from("mssql")
                .executeSource("/mssql/advanced.sql");
    }

    @Test
    void name() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Connection sourceConnection = localCredentials.createSourceConnection()) {
            try (Database sourceDatabase = Database.createDatabase(sourceConnection)) {

                {   // invoices
                    Dependency result = DependencyCalculator.calculateDependencies(sourceDatabase, Dependency.root(invoices));
                    assertDependency(result, "invoices", true, true);

                    // invoices -> customers (SM)
                    Dependency invoicesCustomersFk = result.getSubDependencyByConstraint("invoices_customers_fk").orElseThrow();
                    assertDependency(invoicesCustomersFk, "customers", true, true);

                    // invoices -> customers (SM) -> customer_types (SM)
                    Dependency customersCustomerTypesFk = invoicesCustomersFk.getSubDependencyByConstraint("customers_customer_types_fk").orElseThrow();
                    assertDependency(customersCustomerTypesFk, "customer_types", true, true);

                    // invoices -> invoice_details
                    Dependency invoiceDetailsInvoicesFk = result.getSubDependencyByConstraint("invoice_details_invoices_fk").orElseThrow();
                    assertDependency(invoiceDetailsInvoicesFk, "invoice_details", false, false);
                    assertTrue(invoiceDetailsInvoicesFk.getSubDependencies().isEmpty());
                }


                {   // invoices + invoice_details
                    Dependency dependency = new Dependency(
                            invoices,
                            null,
                            List.of(
                                    new Dependency(
                                            invoiceDetails,
                                            "invoice_details_invoices_fk",
                                            Collections.emptyList(),
                                            true,
                                            false
                                    )
                            ),
                            true,
                            true
                    );
                    Dependency result = DependencyCalculator.calculateDependencies(sourceDatabase, dependency);
                    assertEquals("invoices", result.getTableName().getTable());

                    // invoices -> customers (SM)
                    Dependency invoicesCustomersFk = result.getSubDependencyByConstraint("invoices_customers_fk").orElseThrow();
                    assertDependency(invoicesCustomersFk, "customers", true, true);

                    // invoices -> customers (SM) -> customer_types (SM)
                    Dependency customersCustomerTypesFk = invoicesCustomersFk.getSubDependencyByConstraint("customers_customer_types_fk").orElseThrow();
                    assertDependency(customersCustomerTypesFk, "customer_types", true, true);

                    // invoices -> invoice_details (S)
                    Dependency invoiceDetailsInvoicesFk = result.getSubDependencyByConstraint("invoice_details_invoices_fk").orElseThrow();
                    assertDependency(invoiceDetailsInvoicesFk, "invoice_details", true, false);

                    // invoices -> invoice_details (S) -> products (SM)
                    Dependency invoiceDetailsProductsFk = invoiceDetailsInvoicesFk.getSubDependencyByConstraint("invoice_details_products_fk").orElseThrow();
                    assertDependency(invoiceDetailsProductsFk, "products", true, true);

                    // invoices -> invoice_details (S) -> products (SM) -> product_categories (SM)
                    Dependency productsProductCategoriesFk = invoiceDetailsProductsFk.getSubDependencyByConstraint("products_product_categories_fk").orElseThrow();
                    assertDependency(productsProductCategoriesFk, "product_categories", true, true);
                }
            }
        }
    }

    private static void assertDependency(Dependency dependency, String expectedTable, boolean expectedSelected, boolean expectedMandatory) {
        assertEquals(expectedTable, dependency.getTableName().getTable());
        assertEquals(expectedSelected, dependency.isSelected());
        assertEquals(expectedMandatory, dependency.isMandatory());
    }
}