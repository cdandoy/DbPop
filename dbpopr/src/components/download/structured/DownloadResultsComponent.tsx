import React from "react";
import {DownloadResponse} from "../../../models/DownloadResponse";
import PageHeader from "../../pageheader/PageHeader";
import {tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";
import {NavLink} from "react-router-dom";
import structured_download from "../structured_download.png";

export default function DownloadResultsComponent({downloadResponse}: {
    downloadResponse: DownloadResponse
}) {
    return <>
        <PageHeader title={"Structured Download"} subtitle={"Your data has been dowbloaded"} tool={<img src={structured_download} style={{width: "20em"}} alt={"image"}/>}/>

        <div className={"table-container"}>
            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Table</th>
                    <th className={"text-end"}>Rows</th>
                    <th className={"text-end"}>Skipped</th>
                </tr>
                </thead>
                <tbody>
                {downloadResponse.tableRowCounts.map(tableRowCount => (
                    <tr key={tableNameToFqName(tableRowCount.tableName)}>
                        <td>{tableNameToFqName(tableRowCount.tableName)}</td>
                        <td className={"text-end"}>
                            {tableRowCount.rowCount ? Plural(tableRowCount.rowCount, "row") : '-'}
                        </td>
                        <td className={"text-end"}>
                            {tableRowCount.rowsSkipped ? Plural(tableRowCount.rowsSkipped, "row") : '-'}
                        </td>
                    </tr>
                ))}
                </tbody>
                <tfoot>
                <tr>
                    <th>Total</th>
                    <th className={"text-end"}>
                        {downloadResponse.rowCount ? Plural(downloadResponse.rowCount, "row") : '-'}
                    </th>
                    <th className={"text-end"}>
                        {downloadResponse.rowsSkipped ? Plural(downloadResponse.rowsSkipped, "row") : '-'}
                    </th>
                </tr>
                </tfoot>
            </table>
        </div>

        <div className={"mt-3 mb-3 button-bar"}>
            <NavLink to={"/"} className={"btn btn-primary"}>
                Close
            </NavLink>
        </div>
    </>
}