import React, {useState} from "react"
import StructuredFilterComponent from "./StructuredFilterComponent";
import {TableInfo} from "../../../api/content";
import {TableName, tableNameEquals, tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";
import SelectTableDependenciesComponent from "../SelectTableDependenciesComponent";

export default function SelectTableComponent({
                                                 tableInfos,
                                                 nameFilter, setNameFilter,
                                                 nameRegExp, setNameRegExp,
                                                 dependenciesFilter, setDependenciesFilter,
                                                 tableName, setTableName,
                                                 setPage,
                                             }: {
    tableInfos: TableInfo[],
    nameFilter: string,
    setNameFilter: ((p: string) => void),
    nameRegExp: RegExp,
    setNameRegExp: ((p: RegExp) => void),
    dependenciesFilter: boolean,
    setDependenciesFilter: ((p: boolean) => void),
    tableName: TableName | null,
    setTableName: ((p: TableName) => void),
    setPage: ((p: string) => void),
}) {
    const [showTableNameDependency, setShowTableNameDependency] = useState<TableName | undefined>();

    function filterTableInfo(tableInfo: TableInfo) {
        if (dependenciesFilter && tableInfo.dependencies.length === 0) return false;
        if (!nameRegExp.test(tableNameToFqName(tableInfo.tableName))) return false;
        return true;
    }

    return <>
        <div id={"structured-select-root"}>
            <div className={"mt-3 mb-3 button-bar"}>
                <div className={"btn-group"}>
                    <button className={"btn btn-primary"}
                            disabled={tableName == null}
                            onClick={() => setPage("dependencies")}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </div>
            </div>
            <div className={"mt-3"}>
                <StructuredFilterComponent nameFilter={nameFilter}
                                           setNameFilter={s => {
                                               setNameFilter(s);
                                               setNameRegExp(new RegExp(s));
                                           }}
                                           dependenciesFilter={dependenciesFilter}
                                           setDependenciesFilter={setDependenciesFilter}
                />
                <div className={"table-container"}>
                    <table className={"table table-hover"}>
                        <thead>
                        <tr>
                            <th>Table</th>
                        </tr>
                        </thead>
                        <tbody>
                        {tableInfos
                            .filter(tableInfo => filterTableInfo(tableInfo))
                            .map(tableInfo => {
                                const displayName = tableNameToFqName(tableInfo.tableName);
                                const id = `radio-${displayName}`;

                                return (
                                    <tr key={displayName}>
                                        <td>
                                            <div className="form-check">
                                                <input className="form-check-input"
                                                       type="radio"
                                                       checked={tableName != null && tableNameEquals(tableName, tableInfo.tableName)}
                                                       onChange={() => setTableName(tableInfo.tableName)}
                                                       id={id}
                                                />
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
                                    </tr>
                                )
                            })}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <SelectTableDependenciesComponent tableName={showTableNameDependency} close={() => setShowTableNameDependency(undefined)}/>
    </>
}