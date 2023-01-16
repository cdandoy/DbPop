import axios, {AxiosResponse} from "axios";
import {TableName} from "../models/TableName";
import {DownloadResponse} from "../models/DownloadResponse";

interface DownloadBulkBody {
    dataset: string;
    tableNames: TableName[];
}

export function bulkDownload(dataset: string, tableNames: TableName[]) {
    return axios.post<DownloadBulkBody, AxiosResponse<DownloadResponse>>(`/download/bulk`, {
        dataset: dataset,
        tableNames: tableNames,
    })
}