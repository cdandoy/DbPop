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