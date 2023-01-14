import React, {useEffect, useState} from "react";
import {TableName, tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";
import {content, RowCount, TableInfo} from "../../../api/content";
import Spinner from "../../Spinner";
import './DownloadBulkComponent.scss'
import {bulkDownload} from "../../../api/bulkDownload";
import {DownloadResponse} from "../../../models/DownloadResponse";
import PageHeader from "../../pageheader/PageHeader";
import {FilterComponent} from "./FilterComponent";

export default function SelectTables({
                                         nameFilter, setNameFilter,
                                         nameRegExp, setNameRegExp,
                                         emptyFilter, setEmptyFilter,
                                         downloadedFilter, setDownloadedFilter,
                                         dependenciesFilter, setDependenciesFilter,
                                         dataset, setDataset,
                                         setDownloadResponse
                                     }: {
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
}) {
    const [bulkTables, setBulkTables] = useState<TableName[]>([])
    const [loading, setLoading] = useState(false);
    const [tableInfos, setTableInfos] = useState<TableInfo[]>([])
    const [changeNumber, setChangeNumber] = useState(0);

    useEffect(() => {
        setLoading(true);
        content()
            .then(result => {
                setTableInfos(result.data);
                setLoading(false);
            })
    }, [changeNumber])

    function filterTableInfo(tableInfo: TableInfo) {
        const countRows = (rowCount: RowCount) => rowCount.rows + (rowCount.plus ? 1 : 0);
        if (!emptyFilter && tableInfo.sourceRowCount.rows === 0) return 0;
        if (!nameRegExp.test(tableNameToFqName(tableInfo.tableName))) return false;
        if (!downloadedFilter) {
            const sourceRows = countRows(tableInfo.sourceRowCount);
            const csvRows = countRows(tableInfo.staticRowCount) + countRows(tableInfo.baseRowCount)
            if (sourceRows <= csvRows) return false;
        }
        if (!dependenciesFilter) {
            if (tableInfo.dependencies.length > 0) return false;
        }
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
        bulkDownload(dataset, bulkTables)
            .then(result => {
                setDownloadResponse(result.data);
                setBulkTables([]);
                setChangeNumber(changeNumber + 1);
            })
    }

    if (loading) return <Spinner/>;

    function SelectTables() {
        return <>
            <div id={"select-bulk-tables-component"}>
                <PageHeader title={"Bulk Download"} subtitle={"Download Individual Tables"}/>
                <div className={"mt-3"}>
                    <FilterComponent nameFilter={nameFilter}
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
                                <th>Database Rows</th>
                                <th>Static Rows</th>
                                <th>Base Rows</th>
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
                                                            <span className={"dependency-count"} title={tableInfo.dependencies.map(it => it.table).join(', ')}>
                                                            &nbsp;(+{Plural(tableInfo.dependencies.length, "dependency")})
                                                        </span>
                                                        )}
                                                    </label>
                                                </div>
                                            </td>
                                            <td>
                                                {tableInfo.sourceRowCount.rows}{tableInfo.sourceRowCount.plus ? '+' : ''}
                                            </td>
                                            <td>
                                                {tableInfo.staticRowCount.rows}{tableInfo.staticRowCount.plus ? '+' : ''}
                                            </td>
                                            <td>
                                                {tableInfo.baseRowCount.rows}{tableInfo.baseRowCount.plus ? '+' : ''}
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
                                    <select id={"dataset"} className="form-select" aria-label="Default select example" defaultValue={dataset} onChange={e => setDataset(e.target.value)}>
                                        <option value="static">static</option>
                                        <option value="base">base</option>
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