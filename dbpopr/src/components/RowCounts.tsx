import React, {useEffect, useState} from "react";
import {Dependency} from "../models/Dependency";
import {executeDownload} from "./executeDownload";
import {DownloadResponse} from "../models/DownloadResponse";

export default function RowCounts({changeNumber, dataset, dependency, queryValues, rowCounts, setRowCounts}: {
    dataset: string,
    changeNumber: number,
    dependency: Dependency,
    queryValues: any,
    rowCounts: DownloadResponse | null,
    setRowCounts: ((s: DownloadResponse) => void),
}) {
    const [loading, setLoading] = useState<boolean>(false);

    useEffect(() => {
        if (dependency != null) {
            setLoading(true);
            executeDownload(dataset, dependency, queryValues, true)
                .then(response => {
                    setLoading(false);
                    setRowCounts(response.data);
                })
        }
    }, [changeNumber,dataset, dependency, queryValues, setRowCounts]);

    if (rowCounts == null) return <></>;

    if (loading) return <div><i className={"fa fa-spinner fa-spin"}/> Loading...</div>;

    return (
        <>
            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Table</th>
                    <th>Rows</th>
                </tr>
                </thead>
                <tbody>
                {rowCounts.tableRowCounts.map(tableRowCount =>
                    <tr key={tableRowCount.displayName}>
                        <td>{tableRowCount.displayName}</td>
                        <td>{tableRowCount.rowCount}</td>
                    </tr>
                )}
                </tbody>
            </table>
        </>
    );
}