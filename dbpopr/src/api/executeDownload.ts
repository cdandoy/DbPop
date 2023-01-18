import {Dependency} from "../models/Dependency";
import axios, {AxiosResponse} from "axios";
import {DownloadResponse} from "../models/DownloadResponse";

export function executeDownload(dataset: string, dependency: Dependency, queryValues: any, dryRun: boolean, maxRows: number): Promise<AxiosResponse<DownloadResponse>> {

    function prunedDependency(dependency: Dependency): Dependency {
        return {
            displayName: dependency.displayName,
            tableName: dependency.tableName,
            constraintName: dependency.constraintName,
            subDependencies: !dependency.subDependencies ? null : dependency.subDependencies
                .filter(it => it.selected)
                .map(it => prunedDependency(it)),
            selected: dependency.selected,
            mandatory: dependency.mandatory,
            queries: dependency.queries,
        }
    }

    // Remove the queryValues without a value
    for (let [key, value] of Object.entries(queryValues)) {
        if (!value) {
            delete queryValues[key];
        }
    }

    return axios.post<Dependency, AxiosResponse<DownloadResponse>>(`/download/model`, {
        dataset,
        dependency: prunedDependency(dependency),
        queryValues,
        dryRun: dryRun,
        maxRows: maxRows
    })
}