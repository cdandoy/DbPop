import axios, {AxiosResponse} from "axios";
import {ObjectIdentifier} from "../models/ObjectIdentifier";

export interface Change {
    path: string;
    dbname: string;
    objectIdentifier: ObjectIdentifier;
    fileChanged: boolean;
    databaseChanged: boolean;
    fileDeleted: boolean;
    databaseDeleted: boolean;
}

export const DefaultChanges: Change[] = [];

export function targetChanges(): Promise<AxiosResponse<Change[]>> {
    return axios.get<Change[]>('/code/target/changes')
}