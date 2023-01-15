import React, {useEffect, useState} from "react"
import PageHeader from "../../pageheader/PageHeader";
import BackNextComponent from "../BackNextComponent";
import {Dependency, Query} from "../../../models/Dependency";
import {tableNameEquals, tableNameToFqName} from "../../../models/TableName";
import {executeDownload} from "../../executeDownload";
import {TableRowCounts} from "../../../models/TableRowCounts";
import LoadingOverlay from "../../utils/LoadingOverlay";
import EditDependency from "./EditDependency";

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

export default function DataFilterComponent({setPage, dependency, setDependency}: {
    setPage: ((p: string) => void),
    dependency: Dependency,
    setDependency: ((d: Dependency) => void),
}) {
    const [loading, setLoading] = useState(false);
    const [dependencyAndRowCounts, setDependencyAndRowCounts] = useState<DependencyAndRowCounts[]>([]);
    const [editDependency, setEditDependency] = useState<Dependency | null>(null)

    useEffect(() => {
        setLoading(true);
        const pruned = prunedDependency(dependency)
        executeDownload('static', pruned, {}, true, 1000)
            .then(result => {

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
                setLoading(false);
            })
    }, [dependency]);

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
            <BackNextComponent onBack={() => setPage("dependencies")}/>
            <div className={"table-container"}>
                <table className={"table table-hover"}>
                    <thead>
                    <tr>
                        <th>Table</th>
                        <th>Rows</th>
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
                            <td>
                                {tableRowCounts.rowCount.toLocaleString()}
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