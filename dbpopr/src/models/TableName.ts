export interface TableName {
    catalog: string | null;
    schema: string | null;
    table: string;
}

export function tableNameToFqName(tableName: TableName) {
    let ret = "";
    if (tableName.catalog) ret += tableName.catalog + ".";
    if (tableName.schema) ret += tableName.schema + ".";
    ret += tableName.table;
    return ret;
}

export function tableNameEquals(tableName1: TableName, tableName2: TableName) {
    return tableName1.table === tableName2.table && tableName1.schema === tableName2.schema && tableName1.catalog === tableName2.catalog;
}