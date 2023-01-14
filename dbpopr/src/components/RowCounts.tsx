import React, {useEffect, useState} from "react";
import {Dependency} from "../models/Dependency";
import {executeDownload} from "./executeDownload";
import {DownloadResponse} from "../models/DownloadResponse";
import LoadingOverlay from "./utils/LoadingOverlay";

export default function RowCounts({changeNumber, dataset, dependency, queryValues, downloadResponse, setDownloadResponse, maxRows, setMaxRows}: {
    dataset: string,
    changeNumber: number,
    dependency: Dependency,
    queryValues: any,
    downloadResponse: DownloadResponse | null,
    setDownloadResponse: ((s: DownloadResponse) => void),
    maxRows: number,
    setMaxRows: ((s: number) => void),
}) {
    const [loading, setLoading] = useState<boolean>(false);

    useEffect(() => {
        if (dependency != null && changeNumber > 0) {
            setLoading(true);
            executeDownload(dataset, dependency, queryValues, true, maxRows)
                .then(response => {
                    setLoading(false);
                    setDownloadResponse(response.data);
                })
        }
    }, [changeNumber, dataset, dependency, queryValues, setDownloadResponse, maxRows]);

    if (downloadResponse == null || changeNumber === 0) return <></>;

    let totalRows = 0;
    let totalSkipped = 0;
    for (let tableRowCount of downloadResponse.tableRowCounts) {
        totalRows += tableRowCount.rowCount;
        totalSkipped += tableRowCount.rowsSkipped;
    }

    return (
        <>
            <LoadingOverlay active={loading}/>
            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Table</th>
                    <th className={"text-end"}>Skipped</th>
                    <th className={"text-end"}>Rows</th>
                </tr>
                </thead>
                <tbody>
                {downloadResponse.tableRowCounts.map(tableRowCount =>
                    <tr key={tableRowCount.displayName}>
                        <td>{tableRowCount.displayName}</td>
                        <td className={"text-end"}>{tableRowCount.rowsSkipped.toLocaleString()}</td>
                        <td className={"text-end"}>{tableRowCount.rowCount.toLocaleString()}</td>
                    </tr>
                )}
                </tbody>
                <tfoot>
                <tr>
                    <th>Total</th>
                    <th className={"text-end"}>{totalSkipped.toLocaleString()}</th>
                    <th className={"text-end"}>{totalRows.toLocaleString()}</th>
                </tr>
                </tfoot>
            </table>
            {downloadResponse && downloadResponse.maxRowsReached && (
                <div className={"alert alert-danger"}>
                    &nbsp;
                    <div className={"float-start"}>Row count limit reached: {maxRows.toLocaleString()}</div>
                    <div className={"float-end"}>
                        <button className={"btn btn-sm btn-light"} onClick={() => {
                            setMaxRows(maxRows + 1000)
                        }}>
                            Increase to {(downloadResponse.rowCount + 1000).toLocaleString()}
                        </button>
                    </div>
                </div>
            )}
        </>
    );
}