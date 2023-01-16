import {TableName} from "../models/TableName";
import axios, {AxiosResponse} from "axios";

export interface DatasetContentResponse {
    datasetContents: DatasetContent[];
    tableContents: TableContent[];
}

export interface DatasetContent {
    name: string;
    fileCount: number;
    size: number;
    rows: number;
}

export interface TableContent {
    tableName: TableName;
    content: Record<string, FileContent>;
}

export interface FileContent {
    size: number;
    rows?: number;
}

export function datasetContent(): Promise<AxiosResponse<DatasetContentResponse>> {
    return axios.get<DatasetContentResponse>('/datasets/content');
}