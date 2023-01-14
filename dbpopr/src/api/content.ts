import axios from "axios";
import {TableName} from "../models/TableName";

export interface TableInfo {
    tableName: TableName;
    sourceRowCount: RowCount;
    targetRowCount: RowCount;
    staticRowCount: RowCount;
    baseRowCount: RowCount;
    dependencies: TableName[];
}

export interface RowCount {
    rows: number;
    plus: boolean;
}

export function content() {
    return axios.get<TableInfo[]>('/content');
}