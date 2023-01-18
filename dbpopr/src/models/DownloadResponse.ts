import {TableRowCounts} from "./TableRowCounts";

export interface DownloadResponse {
    tableRowCounts: TableRowCounts[];
    rowCount: number;
    rowsSkipped: number;
    maxRowsReached: boolean;
}