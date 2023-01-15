import {Dependency, Query} from "../../../models/Dependency";
import React, {useEffect, useState} from "react";
import tableApi from "../../../api/tableApi";
import './DownloadModelComponent.scss'

interface QueryColumn {
    column: string;
    indexed: boolean;
    value?: string;
}

export default function EditDependency({dependency, onApply, onCancel}: {
    dependency: Dependency,
    onApply: ((queries: Query[]) => void)
    onCancel: (() => void)
}) {
    const [queryColumns, setQueryColumns] = useState<QueryColumn[]>([])

    useEffect(() => {
        tableApi(dependency.tableName)
            .then(result => {
                const table = result.data;
                const firstIndexedColumns = table.indexes.map(index => index.columns[0]);
                let qcs = table.columns.map(column => {
                    return {
                        column: column.name,
                        indexed: firstIndexedColumns.indexOf(column.name) >= 0,
                        value: dependency.queries?.find(it => it.column === column.name)?.value,
                    };
                });
                setQueryColumns(qcs);
            })
    }, [dependency]);

    function onOk() {
        const queries: Query[] = queryColumns
            .filter(it => it.value)
            .map(it => {
                return {
                    column: it.column,
                    value: it.value!,
                }
            })
        onApply(queries);
    }

    return <>
        <div id={"edit-dependency"}>
            <div className={"table-container"}>
                <table className={"table table-hover"}>
                    <tbody>
                    {queryColumns.map(queryColumn => (
                        <tr key={queryColumn.column}>
                            <td>{queryColumn.column}</td>
                            <td>
                                <input type={"text"}
                                       style={{width: "100%"}}
                                       spellCheck={false}
                                       autoComplete={"off"}
                                       defaultValue={queryColumn.value}
                                       onChange={e => {
                                           const value = e.target.value;
                                           const cloned = structuredClone(queryColumns);
                                           cloned.find(it => it.column === queryColumn.column)!.value = value;
                                           setQueryColumns(cloned);
                                       }}/>
                            </td>
                        </tr>
                    ))}
                    </tbody>
                </table>
            </div>
            <div className={"button-bar"}>
                <button className={"btn btn-default"} onClick={onCancel}>
                    Cancel
                </button>
                <button className={"btn btn-primary"} onClick={onOk}>
                    OK
                </button>
            </div>
        </div>
    </>
}