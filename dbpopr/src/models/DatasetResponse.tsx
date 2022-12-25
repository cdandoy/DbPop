export interface DatasetResponse {
    name: string;
    files: DatasetDatafileResponse[];
}

export interface DatasetDatafileResponse {
    name: string;
    fileSize: number;
    rows: number;
}