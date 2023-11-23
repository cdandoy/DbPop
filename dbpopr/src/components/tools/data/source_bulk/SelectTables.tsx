import React, {useEffect, useState} from "react";
import {TableName, tableNameToFqName} from "../../../../models/TableName";
import {Plural} from "../../../../utils/DbPopUtils";
import {content, RowCount, TableInfo} from "../../../../api/content";
import './DownloadBulkComponent.scss'
import {bulkDownload} from "../../../../api/bulkDownload";
import {DownloadResponse} from "../../../../models/DownloadResponse";
import {FilterComponent} from "./FilterComponent";
import LoadingOverlay from "../../../utils/LoadingOverlay";
import useDatasets from "../../../utils/useDatasets";

export default function SelectTables({
                                         schema, setSchema,
                                         nameFilter, setNameFilter,
                                         nameRegExp, setNameRegExp,
                                         emptyFilter, setEmptyFilter,
                                         downloadedFilter, setDownloadedFilter,
                                         dependenciesFilter, setDependenciesFilter,
                                         dataset, setDataset,
                                         setDownloadResponse,
                                         setShowTableNameDependency
                                     }: {
    schema: string,
    setSchema: ((p: string) => void),
    nameFilter: string,
    setNameFilter: ((p: string) => void),
    nameRegExp: RegExp,
    setNameRegExp: ((p: RegExp) => void),
    emptyFilter: boolean,
    setEmptyFilter: ((p: boolean) => void),
    downloadedFilter: boolean,
    setDownloadedFilter: ((p: boolean) => void),
    dependenciesFilter: boolean,
    setDependenciesFilter: ((p: boolean) => void),
    dataset: string,
    setDataset: ((p: string) => void),
    setDownloadResponse: ((p: DownloadResponse | null) => void),
    setShowTableNameDependency: ((p: TableName | undefined) => void),
}) {
    const [bulkTables, setBulkTables] = useState<TableName[]>([])
    const [loading, setLoading] = useState(false);
    const [tableInfos, setTableInfos] = useState<TableInfo[]>([])
    const [schemas, setSchemas] = useState<string[]>([]);
    const [changeNumber, setChangeNumber] = useState(0);
    const [datasets, loadingDatasets] = useDatasets();

    useEffect(() => {
        setLoading(true);
        content()
            .then(result => {
                const tableInfos: TableInfo[] = result.data;
                setTableInfos(tableInfos);
                const uniqueSchemas = new Set<string>();
                tableInfos.map(it => it.tableName.catalog + "." + it.tableName.schema).forEach(it => uniqueSchemas.add(it));
                setSchemas(Array.from(uniqueSchemas));
                if (tableInfos.length) {
                    if (!uniqueSchemas.has(schema)) {
                        setSchema(uniqueSchemas.values().next().value);
                    }
                }
                setLoading(false);
            })
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [changeNumber])

    function filterTableInfo(tableInfo: TableInfo) {
        const countRows = (rowCount?: RowCount) => rowCount ? rowCount.rows + (rowCount.plus ? 1 : 0) : 0;
        if (!emptyFilter && tableInfo.sourceRowCount && tableInfo.sourceRowCount.rows === 0) return 0;
        if (!downloadedFilter) {
            if (tableInfo.sourceRowCount) {
                const sourceRows = countRows(tableInfo.sourceRowCount);
                const csvRows = countRows(tableInfo.staticRowCount) + countRows(tableInfo.baseRowCount)
                if (sourceRows <= csvRows) return false;
            }
        }
        if (!dependenciesFilter) {
            if (tableInfo.dependencies.length > 0) return false;
        }
        if (schema !== tableInfo.tableName.catalog + "." + tableInfo.tableName.schema) return false;
        if (!nameRegExp.test(tableNameToFqName(tableInfo.tableName))) return false;
        return true;
    }

    function tableNameEq(tn1: TableName, tn2: TableName) {
        return tn1.table === tn2.table && tn1.schema === tn2.schema && tn1.catalog === tn2.catalog;
    }

    function getSelectedTableIndex(tableNames: TableName[], tableName: TableName) {
        return tableNames.findIndex(it => tableNameEq(it, tableName));
    }

    function addTableToSelection(tableNames: TableName[], tableInfo: TableInfo) {
        const tableName = tableInfo.tableName;
        const index = getSelectedTableIndex(tableNames, tableName)
        if (index < 0) {
            tableNames.push(tableName);
            for (const dependentTableName of tableInfo.dependencies) {
                for (const dependentTableInfo of tableInfos) {
                    if (tableNameEq(dependentTableName, dependentTableInfo.tableName)) {
                        addTableToSelection(tableNames, dependentTableInfo);
                    }
                }
            }
        }
    }

    function removeTableFromSelection(tableNames: TableName[], tableInfo: TableInfo) {
        const tableName = tableInfo.tableName;
        const index = getSelectedTableIndex(tableNames, tableName)
        if (index > -1) {
            tableNames.splice(index, 1);
            setBulkTables(tableNames);
        }
        return false;
    }

    function download() {
        setLoading(true);
        bulkDownload(dataset, bulkTables)
            .then(result => {
                setDownloadResponse(result.data);
                setBulkTables([]);
                setChangeNumber(changeNumber + 1);
            })
    }

    function SelectTables() {
        return <>
            <LoadingOverlay active={loading || loadingDatasets}/>
            <div id={"select-bulk-tables-component"}>
                <div className={"mt-3"}>
                    <FilterComponent schemas={schemas}
                                     schema={schema}
                                     setSchema={s => setSchema(s)}
                                     nameFilter={nameFilter}
                                     setNameFilter={s => {
                                         setNameFilter(s);
                                         setNameRegExp(new RegExp(s));
                                     }}
                                     downloaded={downloadedFilter}
                                     setDownloaded={setDownloadedFilter}
                                     empty={emptyFilter}
                                     setEmpty={setEmptyFilter}
                                     dependencies={dependenciesFilter}
                                     setDependencies={setDependenciesFilter}
                    />
                    <div className={"table-container"}>
                        <table className={"table table-hover"}>
                            <thead>
                            <tr>
                                <th>Table</th>
                                <th className={"text-end"}>Database Rows</th>
                                <th className={"text-end"}>Static Rows</th>
                                <th className={"text-end"}>Base Rows</th>
                            </tr>
                            </thead>
                            <tbody>
                            {tableInfos
                                .filter(tableInfo => filterTableInfo(tableInfo))
                                .map(tableInfo => {
                                    const tableName = tableInfo.tableName;
                                    const displayName = tableNameToFqName(tableName);
                                    const id = `check-${displayName}`;
                                    const index = bulkTables.findIndex(it => {
                                        return it.table === tableName.table && it.schema === tableName.schema && it.catalog === tableName.catalog;
                                    });

                                    function onSelectTable(e: React.ChangeEvent<HTMLInputElement>) {
                                        const cloned = Object.assign([], bulkTables);
                                        if (e.target.checked) {
                                            addTableToSelection(cloned, tableInfo)
                                        } else {
                                            removeTableFromSelection(cloned, tableInfo);
                                        }
                                        setBulkTables(cloned);
                                    }

                                    return (
                                        <tr key={displayName}>
                                            <td>
                                                <div className="form-check">
                                                    <input className="form-check-input"
                                                           type="checkbox"
                                                           value=""
                                                           id={id}
                                                           checked={index >= 0}
                                                           onChange={e => onSelectTable(e)}/>
                                                    <label className="form-check-label" htmlFor={id}>
                                                        {displayName}
                                                        {tableInfo.dependencies.length > 0 && (
                                                            <span className={"dependency-count"}
                                                                  onClick={() => setShowTableNameDependency(tableInfo.tableName)}>
                                                            &nbsp;(+<span className={"dependency-word"}>{Plural(tableInfo.dependencies.length, "dependency")}</span>)
                                                        </span>
                                                        )}
                                                    </label>
                                                </div>
                                            </td>
                                            <td className={"text-end"}>
                                                {tableInfo.sourceRowCount && <>{tableInfo.sourceRowCount.rows.toLocaleString()}{tableInfo.sourceRowCount.plus ? '+' : ''}</>}
                                            </td>
                                            <td className={"text-end"}>
                                                {tableInfo.staticRowCount.rows.toLocaleString()}{tableInfo.staticRowCount.plus ? '+' : ''}
                                            </td>
                                            <td className={"text-end"}>
                                                {tableInfo.baseRowCount.rows.toLocaleString()}{tableInfo.baseRowCount.plus ? '+' : ''}
                                            </td>
                                        </tr>
                                    )
                                })}
                            </tbody>
                        </table>
                    </div>
                    <div className={"mt-5 button-bar clearfix"}>
                        <div className={"float-start"}>
                            {Plural(bulkTables.length, 'table')} selected
                        </div>
                        <div className={"float-end"}>
                            <div className="row g-3 align-items-center">
                                <div className="col-auto">
                                    <label className={"col-form-label"} htmlFor={"dataset"}>Dataset:</label>
                                </div>
                                <div className="col-auto">
                                    <select id={"dataset"} className="form-select" defaultValue={dataset} onChange={e => setDataset(e.target.value)}>
                                        {datasets.map(ds => (
                                            <option key={ds} value={ds}>{ds}</option>
                                        ))}
                                    </select>
                                </div>
                                <div className="col-auto">
                                    <button onClick={download} className={"btn btn-primary"} disabled={bulkTables.length === 0}>Download</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </>
    }

    return <>
        <SelectTables/>
    </>
}