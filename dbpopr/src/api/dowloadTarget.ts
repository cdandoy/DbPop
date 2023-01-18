import {Dependency} from "../models/Dependency";
import axios, {AxiosResponse} from "axios";
import {DownloadResponse} from "../models/DownloadResponse";

export function dowloadTarget(dataset: string): Promise<AxiosResponse<DownloadResponse>> {

    return axios.post<Dependency, AxiosResponse<DownloadResponse>>(`/download/target`, {
        dataset,
        dryRun: false,
        maxRows: 2147483648
    })
}