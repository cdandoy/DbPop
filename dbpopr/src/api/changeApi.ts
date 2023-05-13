import axios, {AxiosResponse} from "axios";

export interface Change {
    path: string;
    dbname: string;
    fileChanged: boolean;
    databaseChanged: boolean;
}

export const DefaultChanges: Change[] = [];

export function targetChanges(): Promise<AxiosResponse<Change[]>> {
    return axios.get<Change[]>('/code/target/changes')
}