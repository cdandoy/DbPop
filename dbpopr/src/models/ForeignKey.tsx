import {TableName} from "./TableName";

export interface ForeignKey {
    name: string;
    pkTableName: TableName;
    pkColumns: string[];
    fkTableName: TableName;
    fkColumns: string[];
}