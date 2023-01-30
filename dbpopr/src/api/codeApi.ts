import axios from "axios";
import {TableName} from "../models/TableName";

export interface CodeDiff {
    entries: CodeDiffEntry[]
}

export interface CodeDiffEntry {
    tableName: TableName;
    type: string;
    databaseTime: number | undefined;
    fileTime: number | undefined;
}

export interface UploadResult {
    fileExecutions: FileExecution[];
    executionTime: number;
}

export interface DownloadResult {
    codeTypeCounts: Pair[];
    executionTime: number;
}

export interface Pair {
    left: any;
    right: any;
}

export interface FileExecution {
    filename: string;
    error: string | undefined;
}

export function compareSourceToFile() {
    return axios.get<CodeDiff>(`/code/source/compare`);
}

export function downloadSourceToFile() {
    return axios.get<DownloadResult>(`/code/source/download`);
}

export function compareTargetToFile() {
    return axios.get<CodeDiff>(`/code/target/compare`);
}

export function uploadFileToTarget() {
    return axios.get(`/code/target/upload`);
}

export function downloadTargetToFile() {
    return axios.get(`/code/target/download`);
}

