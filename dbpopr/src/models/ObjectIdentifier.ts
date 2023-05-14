import {TableName} from "./TableName";

export interface ObjectIdentifier {
    type: string
    tableName: TableName
    parent?: ObjectIdentifier
}