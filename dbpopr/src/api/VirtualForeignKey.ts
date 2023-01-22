import axios from "axios";
import {ForeignKey} from "../models/ForeignKey";
import {TableName} from "../models/TableName";

export function getVirtualForeignKey(tableName: TableName, fkName: string) {
    return axios.get<ForeignKey>(`/database/vfks/${tableName.catalog}/${tableName.schema}/${tableName.table}/${fkName}`)
}

export function getVirtualForeignKeys(tableName: TableName) {
    return axios.get<ForeignKey[]>(`/database/vfks/${tableName.catalog}/${tableName.schema}/${tableName.table}`)
}

export function saveVirtualForeignKey(name: string, pkTableName: TableName, pkTableColumns: string[], fkTableName: TableName, fkTableColumns: string[]) {
    return axios.post<ForeignKey>('/database/vfks', {
        name: name,
        pkTableName: pkTableName,
        pkColumns: pkTableColumns,
        fkTableName: fkTableName,
        fkColumns: fkTableColumns
    })
}