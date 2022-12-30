import React, {useEffect, useState} from "react";
import {Dependency} from "./Dependency";
import {DownloadResponse, executeDownload} from "./downloadApi";


export default function RowCounts({changeNumber, dataset, dependency, queryValues, rowCounts,setRowCounts }: {
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
    }, [changeNumber]);

    if (rowCounts == null) return <></>;

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