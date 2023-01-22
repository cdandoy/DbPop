import {TableName} from "../models/TableName";
import axios from "axios";
import {Table} from "../models/Table";

export function getTable(tableName: TableName) {
    return axios.get<Table>(`/database/tables/${tableName.catalog}/${tableName.schema}/${tableName.table}`);
}