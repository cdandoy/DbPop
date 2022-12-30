import {TableName} from "../models/TableName";
import {Dependency} from "./Dependency";
import axios, {AxiosResponse} from "axios";

export interface DownloadResponse {
    tableRowCounts: TableRowCounts[];
    rowCount: number;
}

export interface TableRowCounts {
    displayName: string;
    tableName: TableName;
    rowCount: number;
}

export function executeDownload(dataset: string, dependency: Dependency, queryValues: any, dryRun: boolean): Promise<AxiosResponse<DownloadResponse>> {

    for (let [key, value] of Object.entries(queryValues)) {
        if (!value) {
            delete queryValues[key];
        }
    }

    return axios.post<Dependency, AxiosResponse<DownloadResponse>>(`/download`, {
        dataset,
        dependency,
        queryValues,
        dryRun: dryRun
    })
}