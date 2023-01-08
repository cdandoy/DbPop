import {Dependency} from "../models/Dependency";
import axios, {AxiosResponse} from "axios";
import {DownloadResponse} from "../models/DownloadResponse";

export function executeDownload(dataset: string, dependency: Dependency, queryValues: any, dryRun: boolean, maxRows: number): Promise<AxiosResponse<DownloadResponse>> {

    for (let [key, value] of Object.entries(queryValues)) {
        if (!value) {
            delete queryValues[key];
        }
    }

    return axios.post<Dependency, AxiosResponse<DownloadResponse>>(`/download`, {
        dataset,
        dependency,
        queryValues,
        dryRun: dryRun,
        maxRows:maxRows
    })
}