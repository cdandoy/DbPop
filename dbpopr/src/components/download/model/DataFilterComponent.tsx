import React, {useEffect, useState} from "react"
import PageHeader from "../../pageheader/PageHeader";
import {Dependency, Query} from "../../../models/Dependency";
import {tableNameEquals, tableNameToFqName} from "../../../models/TableName";
import {TableRowCounts} from "../../../models/TableRowCounts";
import LoadingOverlay from "../../utils/LoadingOverlay";
import EditDependency from "./EditDependency";
import {DownloadResponse} from "../../../models/DownloadResponse";
import {executeDownload} from "../../../api/executeDownload";

interface DependencyAndRowCounts {
    dependency: Dependency;
    tableRowCounts: TableRowCounts
}

function findDependency(dependency: Dependency, search: Dependency): Dependency | undefined {
    if (dependency.constraintName === search.constraintName && tableNameEquals(dependency.tableName, search.tableName)) return dependency;
    if (dependency.subDependencies) {
        for (const subDependency of dependency.subDependencies) {
            const found = findDependency(subDependency, search);
            if (found) {
                return found;
            }
        }
    }
}

export default function DataFilterComponent({setPage, datasets, dataset, setDataset, dependency, setDependency, setDownloadResponse}: {
    setPage: ((p: string) => void),
    datasets: string[],
    dataset: string,
    setDataset: ((d: string) => void),
    dependency: Dependency,
    setDependency: ((d: Dependency) => void),
    setDownloadResponse: ((p: DownloadResponse) => void),
}) {
    const [loading, setLoading] = useState(false);
    const [previewResponse, setPreviewResponse] = useState<DownloadResponse | undefined>();
    const [dependencyAndRowCounts, setDependencyAndRowCounts] = useState<DependencyAndRowCounts[]>([]);
    const [editDependency, setEditDependency] = useState<Dependency | null>(null)
    const [rowLimit, setRowLimit] = useState(1000);

    useEffect(() => {
        setLoading(true);
        const pruned = prunedDependency(dependency)
        executeDownload(dataset, pruned, {}, true, rowLimit)
            .then(result => {
                setPreviewResponse(result.data);
                const selectedDependencies: Dependency[] = [];
                pushSelectedDependency(selectedDependencies, dependency);
                selectedDependencies.sort((a, b) => {
                    const s1 = tableNameToFqName(a.tableName).toUpperCase();
                    const s2 = tableNameToFqName(b.tableName).toUpperCase();
                    if (s1 < s2) return -1;
                    if (s1 > s2) return 1;
                    return 0;
                });

                const tableRowCounts = result.data.tableRowCounts;
                const ret: DependencyAndRowCounts[] = []
                selectedDependencies.forEach(selectedDependency => {
                    const tableName = selectedDependency.tableName;
                    const tableRowCount = tableRowCounts.find(it => tableNameEquals(it.tableName, tableName));
                    if (tableRowCount) {
                        ret.push({
                            dependency: selectedDependency,
                            tableRowCounts: tableRowCount,
                        })
                    }
                });
                setDependencyAndRowCounts(ret);
            })
            .finally(()=>{
                setLoading(false);
            })
    }, [dependency, dataset, rowLimit]);

    function onDownload() {
        setLoading(true);
        const pruned = prunedDependency(dependency)
        executeDownload(dataset, pruned, {}, false, rowLimit)
            .then(result => {
                setDownloadResponse(result.data);
                setPage("download-result");
            });
    }

    function prunedDependency(dependency: Dependency): Dependency {
        return {
            displayName: dependency.displayName,
            tableName: dependency.tableName,
            constraintName: dependency.constraintName,
            subDependencies: !dependency.subDependencies ? null : dependency.subDependencies
                .filter(it => it.selected)
                .map(it => prunedDependency(it)),
            selected: dependency.selected,
            mandatory: dependency.mandatory,
            queries: dependency.queries,
        }
    }

    function pushSelectedDependency(selected: Dependency[], dependency: Dependency) {
        if (dependency.selected) {
            selected.push(dependency);
            if (dependency.subDependencies) {
                for (const subDependency of dependency.subDependencies) {
                    pushSelectedDependency(selected, subDependency);
                }
            }
        }
    }

    function getFilter(queries: Query[]) {
        return queries
            .map(query => `${query.column} = ${query.value}`)
            .join(' AND ')
    }

    return <div id={"data-filter"}>
        <LoadingOverlay active={loading}/>
        <PageHeader title={"Model Download"} subtitle={"Filter the data"}/>
        {editDependency == null && <>
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

            <div className={"row"}>
                <div className={"col-3"}>
                    <label htmlFor="dataset" className="form-label">Dataset:</label>
                    <select id={"dataset"} className="form-select" aria-label="Dataset" defaultValue={dataset} onChange={e => setDataset(e.target.value)}>
                        {datasets.map(ds => (
                            <option key={ds} value={ds}>{ds}</option>
                        ))}
                    </select>
                </div>
            </div>

            <div className={"table-container mt-3"}>
                <table className={"table table-hover"}>
                    <thead>
                    <tr>
                        <th>Table</th>
                        <th className={"text-end"}>Rows</th>
                        <th className={"text-end"}>Skipped</th>
                        <th>Filter</th>
                        <th></th>
                    </tr>
                    </thead>
                    <tbody>
                    {dependencyAndRowCounts.map(dependencyAndRowCount => {
                        const tableRowCounts = dependencyAndRowCount.tableRowCounts;
                        const tableName = dependencyAndRowCount.dependency.tableName;
                        return <tr key={tableNameToFqName(tableName)}>
                            <td>
                                {tableNameToFqName(tableName)}
                            </td>
                            <td className={"text-end"}>
                                {tableRowCounts.rowCount.toLocaleString()}
                            </td>
                            <td className={"text-end"}>
                                {tableRowCounts.rowsSkipped.toLocaleString()}
                            </td>
                            <td>
                                <button className={"btn btn-xs btn-primary"} onClick={() => setEditDependency(dependencyAndRowCount.dependency)}>
                                    <i className={"fa fa-edit"}/>
                                </button>
                                <span className={"filters"}>
                                    {dependencyAndRowCount.dependency.queries && (
                                        <span className={"filter"}>
                                            {getFilter(dependencyAndRowCount.dependency.queries)}
                                        </span>
                                    )}
                                </span>
                            </td>
                        </tr>
                    })}
                    </tbody>
                    <tfoot>
                    <tr>
                        <th>Total</th>
                        <th className={"text-end"}>{previewResponse?.rowCount}</th>
                        <th className={"text-end"}>
                            {previewResponse?.tableRowCounts?.map(it => it.rowsSkipped).reduce((previousValue, currentValue) => previousValue + currentValue, 0)}
                        </th>
                        <th></th>
                    </tr>
                    </tfoot>
                </table>
            </div>
        </>}

        {editDependency != null && <EditDependency dependency={editDependency}
                                                   onApply={(queries) => {
                                                       const clone = structuredClone(dependency);
                                                       const clonedDependency = findDependency(clone, editDependency);
                                                       if (clonedDependency) {
                                                           clonedDependency.queries = queries;
                                                           setDependency(clone);
                                                       }
                                                       setEditDependency(null);
                                                   }}
                                                   onCancel={() => {
                                                       setEditDependency(null);
                                                   }}
        />}
    </div>
}