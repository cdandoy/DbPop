import axios from "axios";

export interface GetDeployResponse {
    hasChanges: boolean;
    timestamp: number;
    snapshotFilename: string;
    deltaType: string | undefined;
}

export function getDeploy() {
    return axios.get<GetDeployResponse>("/deploy");
}

export function createSnapshot(deltaType: string | undefined = undefined) {
    return axios.post("/deploy/snapshot", {deltaType});
}