import {TableRowCounts} from "./TableRowCounts";

export interface DownloadResponse {
    tableRowCounts: TableRowCounts[];
    rowCount: number;
    maxRowsReached: boolean;
}