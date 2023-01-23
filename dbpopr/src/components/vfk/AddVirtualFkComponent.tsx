import React, {useEffect, useState} from "react";
import {SelectTable} from "../SelectTable";
import {useNavigate, useParams} from "react-router-dom";
import {TableName} from "../../models/TableName";
import {Column, Index, Table} from "../../models/Table";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import SelectedColumns from "./SelectedColumns";
import {getVirtualForeignKey, saveVirtualForeignKey} from "../../api/VirtualForeignKey";
import {getTable} from "../../api/Table";
import SaveSection from "./SaveSection";

export default function AddVirtualFkComponent() {
    const params = useParams();
    const editedPkTable = params.pkTable;
    const editedFkName = params.fkName;
    const [loading, setLoading] = useState<boolean>(true)
    const [saving, setSaving] = useState<boolean>(false)
    const [error, setError] = useState<string | null>('')
    const [name, setName] = useState<string>('');
    const [placeholderName, setPlaceholderName] = useState<string>('');
    // Selected PK table
    const [pkTableName, setPkTableName] = useState<TableName | undefined>(getTableName(params.pkTable));
    // All columns of the selected PK table
    const [allPkTableColumns, setAllPkTableColumns] = useState<Column[]>([]);
    // Selected PK columns
    const [pkTableColumns, setPkTableColumns] = useState<string[]>([]);
    const [uniqueIndex, setUniqueIndex] = useState<Index | null>(null);

    // Selected FK table
    const [fkTableName, setFkTableName] = useState<TableName | undefined>();
    // All columns of the selected FK table
    const [allFkTableColumns, setAllFkTableColumns] = useState<Column[]>([]);
    // Selected FK columns
    const [fkTableColumns, setFkTableColumns] = useState<string[]>([]);
    const navigate = useNavigate();

    function getTableName(tableString?: string): TableName | undefined {
        if (!tableString) return undefined;
        let pkParts = tableString.split('.');
        return {catalog: pkParts[0], schema: pkParts[1], table: pkParts[2],}
    }

    useEffect(() => {
        if (!(pkTableName && editedFkName)) {
            setLoading(false);
            return;
        }
        getTable(pkTableName)
            .then(result => {
                const pkTable: Table = result.data;
                if (pkTable) {
                    const pkTableName = pkTable.tableName;
                    // Get the FK info
                    getVirtualForeignKey(pkTableName, editedFkName)
                        .then((result) => {
                            let fk = result.data;
                            if (fk) {
                                const fkTableName = fk.fkTableName;
                                getTable(fkTableName)
                                    .then((result) => {
                                        const fkTable = result.data;
                                        if (fkTable) {
                                            setAllPkTableColumns(pkTable.columns);
                                            setPkTableName(pkTableName);
                                            setName(fk.name);
                                            setFkTableName(fkTableName);
                                            const columnNames: string[] = fk.pkColumns;
                                            setPkTableColumns(columnNames);
                                            setUniqueIndexIfOnlyOne(pkTable);
                                            setFkTableColumns(fk.fkColumns);
                                            setAllFkTableColumns(fkTable.columns);
                                            setLoading(false);
                                        }
                                    })
                            }
                        })
                }
            });
    }, [editedFkName, editedPkTable]);

    useEffect(() => {
        if (pkTableName && fkTableName) {
            setPlaceholderName(pkTableName.table + '_' + fkTableName.table + '_fk')
        }
    }, [pkTableName, fkTableName])

    function setUniqueIndexIfOnlyOne(pkTable: Table) {
        const uniqueIndexes = pkTable.indexes?.filter(it => it.unique);
        if (uniqueIndexes && uniqueIndexes.length) {
            const uniqueIndex = uniqueIndexes[0];
            setUniqueIndex(uniqueIndex);
            setPkTableColumns(uniqueIndex.columns);
        }
    }

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        setSaving(true);
        setError(null);
        saveVirtualForeignKey(
            name.length ? name : placeholderName,
            pkTableName!,
            pkTableColumns,
            fkTableName!,
            fkTableColumns
        )
            .then(() => navigate("/vfk"))
            .catch(error => {
                setError(error.response.data?.detail || error.message);
                setSaving(false);
            });
    }

    return (
        <>
            <LoadingOverlay active={loading}/>
            <PageHeader title={"Virtual Foreign Keys"}/>
            <form onSubmit={event => whenSave(event)}>
                <div className={"row"}>
                    <div className={"col-8"}>
                        <label htmlFor="fkName" className="form-label">Name:</label>
                        <input className="form-control"
                               name={"fkName"}
                               placeholder={placeholderName}
                               type={"text"}
                               value={name}
                               autoComplete={"off"}
                               onChange={(event) => setName(event.target.value)}/>
                    </div>
                </div>
                <div className={"row mt-4"}>
                    <div className={"col-4"}>
                        <div>Primary Key Table:</div>
                        <div>
                            <SelectTable
                                setTable={(table) => {
                                    setPkTableName(table.tableName);
                                    setAllPkTableColumns(table.columns);
                                    setUniqueIndexIfOnlyOne(table);
                                }}
                            />
                        </div>

                        {uniqueIndex == null && (
                            <SelectedColumns allTableColumns={allPkTableColumns}
                                             selectedColumns={pkTableColumns}
                                             setSelectedColumns={setPkTableColumns}
                            />
                        )}

                        {uniqueIndex != null && (
                            uniqueIndex.columns.map(column => (
                                <div key={column} className={"ms-4 mt-2"}>
                                    <span className={"form-control"}>{column}</span>
                                </div>
                            )))}
                    </div>
                    <div className={"col-4"}>
                        <div>Foreign Key Table:</div>
                        <div>
                            <SelectTable setTable={(table) => {
                                setFkTableName(table.tableName);
                                setAllFkTableColumns(table.columns);
                            }}
                            />
                        </div>
                        {pkTableColumns && <SelectedColumns allTableColumns={allFkTableColumns}
                                                            selectedColumns={fkTableColumns}
                                                            setSelectedColumns={setFkTableColumns}
                                                            matchColumns={pkTableColumns}/>
                        }
                    </div>
                </div>
                <div className={"row"}>
                    <div className={"col-8 mt-4"}>
                        {error && <div className="alert alert-danger start"><i className="fa fa-error"/> {error}</div>}
                        <SaveSection pkTableName={pkTableName}
                                     pkTableColumns={pkTableColumns}
                                     fkTableName={fkTableName}
                                     fkTableColumns={fkTableColumns}
                                     saving={saving}/>
                    </div>
                </div>
            </form>
        </>
    )
}