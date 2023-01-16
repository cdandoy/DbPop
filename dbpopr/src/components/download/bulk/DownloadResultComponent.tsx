import React from "react";
import {DownloadResponse} from "../../../models/DownloadResponse";
import PageHeader from "../../pageheader/PageHeader";
import {tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";

export default function DownloadResultComponent({downloadResponse, setDownloadResponse}: {
    downloadResponse: DownloadResponse;
    setDownloadResponse: ((p: DownloadResponse | null) => void);
}) {
    return <>
        <PageHeader title={"Bulk Download"} subtitle={"Download Individual Tables"}/>
        <div>
            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Table</th>
                    <th>Rows</th>
                </tr>
                </thead>
                <tbody>
                {downloadResponse.tableRowCounts.map(tableRowCount => (
                    <tr key={tableNameToFqName(tableRowCount.tableName)}>
                        <td>{tableNameToFqName(tableRowCount.tableName)}</td>
                        <td>{Plural(tableRowCount.rowCount, "row")}</td>
                    </tr>
                ))}
                </tbody>
                <tfoot>
                    <tr>
                        <th>Total</th>
                        <th>{Plural(downloadResponse.rowCount, "row")}</th>
                    </tr>
                </tfoot>
            </table>
        </div>
        <div className={"clearfix mt-5"}>
            <div className={"float-end"}>
                <button className={"btn btn-primary"} onClick={() => setDownloadResponse(null)}>
                    Close
                </button>
            </div>
        </div>
    </>
}