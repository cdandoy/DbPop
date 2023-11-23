import React, {useEffect, useState} from "react"
import {Dependency, Query} from "../../../../models/Dependency";
import {TableName, tableNameEquals, tableNameToFqName} from "../../../../models/TableName";
import EditDependency from "./EditDependency";
import {DownloadResponse} from "../../../../models/DownloadResponse";

export interface DependencyQuery {
    tableName: TableName,
    constraintName: string | null,
    queries: Query[],
}

interface DataFilterRow {
    tableName: TableName;
    constraintName: string | null;
    rows?: number;
    skipped?: number;
    queries: Query[];
}

export default function DataFilterComponent({
                                                setPage, datasets,
                                                dataset, setDataset,
                                                autoRefresh, setAutoRefresh,
                                                refresh,
                                                dirty, setDirty,
                                                rowLimit, setRowLimit,
                                                dependency,
                                                dependencyQueries, setDependencyQueries,
                                                previewResponse,
                                                onDownload,
                                                error
                                            }: {
    setPage: ((p: string) => void),
    datasets: string[],
    dataset: string,
    setDataset: ((d: string) => void),
    autoRefresh: boolean,
    setAutoRefresh: ((p: boolean) => void),
    refresh: (() => void),
    dirty: boolean,
    setDirty: ((p: boolean) => void),
    rowLimit: number,
    setRowLimit: ((p: number) => void),
    dependency: Dependency,
    dependencyQueries: DependencyQuery[],
    setDependencyQueries: ((p: DependencyQuery[]) => void),
    previewResponse: DownloadResponse | undefined,
    onDownload: (() => void),
    error: string | undefined
}) {
    const [editDependencyQuery, setEditDependencyQuery] = useState<DependencyQuery | null>(null);
    const [dataFilterRows, setDataFilterRows] = useState<DataFilterRow[]>([]);

    function flattenDependency(dependency: Dependency, dataFilterRows: DataFilterRow[]) {
        if (dependency.selected) {
            dataFilterRows.push({
                tableName: dependency.tableName,
                constraintName: dependency.constraintName,
                queries: [],
            });
            if (dependency.subDependencies) {
                for (const subDependency of dependency.subDependencies) {
                    flattenDependency(subDependency, dataFilterRows);
                }
            }
        }
    }

    useEffect(() => {
        if (previewResponse) {
            setDirty(false);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [previewResponse]);

    useEffect(() => {
        const dataFilterRows: DataFilterRow[] = [];
        // Collect the table and constraint names from the dependency tree
        flattenDependency(dependency, dataFilterRows);
        dataFilterRows.sort((a, b) => {
            const s1 = tableNameToFqName(a.tableName).toUpperCase();
            const s2 = tableNameToFqName(b.tableName).toUpperCase();
            if (s1 < s2) return -1;
            if (s1 > s2) return 1;
            return 0;
        })
        // Collect the query values
        for (const dependencyQuery of dependencyQueries) {
            for (const dataFilterRow of dataFilterRows) {
                if (dependencyQuery.constraintName === dataFilterRow.constraintName && tableNameEquals(dataFilterRow.tableName, dependencyQuery.tableName)) {
                    dataFilterRow.queries = dependencyQuery.queries;
                }
            }
        }

        // Collect the row counts
        if (previewResponse) {
            for (const tableRowCount of previewResponse.tableRowCounts) {
                for (const dataFilterRow of dataFilterRows) {
                    if (tableNameEquals(dataFilterRow.tableName, tableRowCount.tableName)) {
                        dataFilterRow.rows = tableRowCount.rowCount;
                        dataFilterRow.skipped = tableRowCount.rowsSkipped;
                    }
                }
            }
        }
        // Eon
        setDataFilterRows(dataFilterRows);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [dependency, dependencyQueries, previewResponse])

    return <div id={"data-filter"}>
        {editDependencyQuery == null && <>
            <div className={"mt-3 mb-3 button-bar"}>
                <div className={"btn-group"}>
                    <button className={"btn btn-primary"} onClick={() => setPage("dependencies")}>
                        <i className={"fa fa-arrow-left"}/>
                        &nbsp;
                        Back
                    </button>
                    <button className={"btn btn-primary"} onClick={onDownload} disabled={previewResponse?.maxRowsReached || previewResponse?.rowCount === 0}>
                        Download
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </div>
            </div>

            {previewResponse?.maxRowsReached && (
                <div className={"alert alert-danger clearfix"}>
                    <div className={"float-start "}>Row limit reached: {rowLimit}</div>
                    <div className={"float-end"}>
                        <button className={"btn btn-primary btn-sm"} onClick={() => {
                            setRowLimit(rowLimit + 1000);
                        }}>
                            Increase
                        </button>
                    </div>
                </div>
            )}

            {previewResponse?.rowCount === 0 && (
                <div className={"alert alert-danger"}>
                    Nothing to download
                </div>
            )}

            {error && (
                <div className={"alert alert-danger"}>
                    {error}
                </div>
            )}

            <div className={"row"}>
                <div className={"col-3"}>
                    <label htmlFor="dataset" className="form-label">Dataset:</label>
                    <select id={"dataset"} className="form-select" aria-label="Dataset" defaultValue={dataset} onChange={e => setDataset(e.target.value)}>
                        {datasets.map(ds => (
                            <option key={ds} value={ds}>{ds}</option>
                        ))}
                    </select>
                </div>
                <div className={"col-2"}>
                    <div className="form-check" style={{paddingTop: "37px"}}>
                        <input className="form-check-input"
                               type="checkbox"
                               checked={autoRefresh}
                               onChange={e => setAutoRefresh(e.target.checked)}
                               id={"option-preview"}/>
                        <label className="form-check-label" htmlFor="option-preview">
                            Auto-Refresh
                        </label>
                        {!autoRefresh && (
                            <button className={"btn btn-sm btn-primary"}
                                    style={{marginLeft: "1em"}}
                                    disabled={!dirty}
                                    onClick={refresh}>
                                <i className={"fa fa-refresh"}/>
                            </button>
                        )}
                    </div>
                </div>
            </div>

            <div className={"table-container mt-3"}>
                <table className={"table table-hover" + (dirty ? ' dirty' : '')}>
                    <thead>
                    <tr>
                        <th>Table</th>
                        <th className={"text-end count"}>Rows</th>
                        <th className={"text-end count"}>Skipped</th>
                        <th>Filter</th>
                        <th></th>
                    </tr>
                    </thead>
                    <tbody>
                    {dataFilterRows.map(dataFilterRow => {
                        return (
                            <tr key={tableNameToFqName(dataFilterRow.tableName) + '-' + dataFilterRow.constraintName}>
                                <td>
                                    {tableNameToFqName(dataFilterRow.tableName)}
                                </td>
                                <td className={"text-end count"}>
                                    {dataFilterRow.rows ? dataFilterRow.rows.toLocaleString() : '-'}
                                </td>
                                <td className={"text-end count"}>
                                    {dataFilterRow.skipped ? dataFilterRow.skipped.toLocaleString() : '-'}
                                </td>
                                <td>
                                    <button className={"btn btn-xs btn-primary"} onClick={() => {
                                        setEditDependencyQuery({
                                            tableName: dataFilterRow.tableName,
                                            constraintName: dataFilterRow.constraintName,
                                            queries: dataFilterRow.queries || []
                                        })
                                    }}>
                                        <i className={"fa fa-filter"}/>
                                    </button>
                                    <span className={"filters"}>
                                    {dataFilterRow.queries && (
                                        <span className={"filter"}>
                                            {dataFilterRow.queries
                                                .map(query => `${query.column} = ${query.value}`)
                                                .join(' AND ')}
                                        </span>
                                    )}
                                </span>
                                </td>
                            </tr>
                        )
                    })}
                    </tbody>
                    <tfoot>
                    <tr>
                        <th>Total</th>
                        <th className={"text-end count"}>{previewResponse?.rowCount}</th>
                        <th className={"text-end count"}>
                            {previewResponse?.tableRowCounts?.map(it => it.rowsSkipped).reduce((previousValue, currentValue) => previousValue + currentValue, 0)}
                        </th>
                        <th></th>
                    </tr>
                    </tfoot>
                </table>
            </div>
        </>}

        {editDependencyQuery != null && (
            <EditDependency dependencyQuery={editDependencyQuery}
                            setDependencyQuery={dependencyQuery => {
                                const clone = dependencyQueries.filter(dq => !(dq.constraintName === dependencyQuery.constraintName && tableNameEquals(dq.tableName, dependencyQuery.tableName)))
                                clone.push(dependencyQuery);
                                setDependencyQueries(clone);
                                setDirty(true);
                                setEditDependencyQuery(null);
                            }}
                            onCancel={() => setEditDependencyQuery(null)}
            />
        )}
    </div>
}