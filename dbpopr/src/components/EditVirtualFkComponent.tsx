import React, {useEffect, useState} from "react";
import {SelectTable} from "./SelectTable";
import axios from "axios";
import {ForeignKey} from "../models/ForeignKey";
import {NavLink, useNavigate, useParams} from "react-router-dom";
import {TableName, tableNameToFqName} from "../models/TableName";
import {Index, Table} from "../models/Table";

function SelectedColumns({allTableColumns, selectedColumns, setSelectedColumns, matchColumns}: {
    allTableColumns: string[] | null,
    selectedColumns: string[],
    setSelectedColumns: (s: string[]) => void,
    matchColumns: string[] | undefined | null;
}) {
    if (!allTableColumns) return <></>;

    let rows: string[] = [];
    if (!matchColumns) {
        rows = selectedColumns.filter(it => it.length).concat(['']);
    } else {
        selectedColumns = selectedColumns.slice(0, matchColumns.length);
        matchColumns = matchColumns.filter(it => it.length);
        while (rows.length < matchColumns.length && rows.length < selectedColumns.length) {
            rows.push(selectedColumns[rows.length]);
        }
        while (rows.length < matchColumns.length) {
            rows.push('');
        }
    }
    const availableColumns = [''].concat(allTableColumns);

    return (
        <>
            {rows.map((selectedColumn, i) => {
                return (
                    <div key={i} className={"ms-4 mt-2"}>
                        <select className="form-select"
                                aria-label="Column"
                                defaultValue={selectedColumn}
                                onChange={event => {
                                    rows[i] = event.target.value;
                                    // rows = rows.filter(it => it.length);
                                    setSelectedColumns(rows);
                                }}>
                            {availableColumns
                                .filter(column => column === '' || rows.slice(0, i).indexOf(column) === -1) // Do not include already selected columns
                                .map((column, i) => {
                                    return (
                                        <option key={i} value={column}>{column}</option>
                                    )
                                })}
                        </select>
                    </div>
                );
            })}
        </>
    )
}

function IndexColumns({columns}: { columns: string[] }) {
    return <>
        {columns.map(column => (
            <div key={column} className={"ms-4 mt-2"}>
                <span className={"form-control"}>{column}</span>
            </div>
        ))}
    </>
}

export default function EditVirtualFkComponent() {
    const params = useParams();
    const editedPkTable = params.pkTable;
    const editedFkName = params.fkName;
    const [loading, setLoading] = useState<boolean>(true)
    const [saving, setSaving] = useState<boolean>(false)
    const [error, setError] = useState<string | null>('')
    const [name, setName] = useState<string>('');
    const [placeholderName, setPlaceholderName] = useState<string>('');
    // Selected PK table
    const [pkTableName, setPkTableName] = useState<TableName | null>(null);
    // All columns of the selected PK table
    const [allPkTableColumns, setAllPkTableColumns] = useState<string[] | null>(null);
    // Selected PK columns
    const [pkTableColumns, setPkTableColumns] = useState<string[]>([]);
    const [uniqueIndex, setUniqueIndex] = useState<Index | null>(null);

    // Selected FK table
    const [fkTableName, setFkTableName] = useState<TableName | null>(null);
    // All columns of the selected FK table
    const [allFkTableColumns, setAllFkTableColumns] = useState<string[] | null>(null);
    // Selected FK columns
    const [fkTableColumns, setFkTableColumns] = useState<string[]>([]);
    const navigate = useNavigate();

    useEffect(() => {
        if (!(editedPkTable && editedFkName)) {
            setLoading(false);
            return;
        }
        let pkParts = editedPkTable.split('.');
        axios.get<Table | null>(`/database/tables/${pkParts[0]}/${pkParts[1]}/${pkParts[2]}`)
            .then(result => {
                let pkTable = result.data;
                if (pkTable) {
                    const pkTableName = pkTable.tableName;
                    // Get the FK info
                    axios.get<ForeignKey>(`/database/vfks/${pkTableName.catalog}/${pkTableName.schema}/${pkTableName.table}/${editedFkName}`)
                        .then((result) => {
                            let fk = result.data;
                            if (fk) {
                                const fkTableName = fk.fkTableName;
                                axios.get<Table | null>(`/database/tables/${fkTableName.catalog}/${fkTableName.schema}/${fkTableName.table}`)
                                    .then((result) => {
                                        const fkTable = result.data;
                                        if (fkTable) {
                                            setAllPkTableColumns(pkTable!.columns.map(it => it.name));
                                            setPkTableName(pkTableName);
                                            setName(fk.name);
                                            setFkTableName(fkTableName);
                                            setPkTableColumns(fk.pkColumns);
                                            setUniqueIndexIfOnlyOne(pkTable!);
                                            setFkTableColumns(fk.fkColumns);
                                            setAllFkTableColumns(fkTable.columns.map(it => it.name));
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

    function cannotSaveMessage(): string | null {
        if (!pkTableName) return 'Please select a primary key table';
        const pkTableColumnsLength = pkTableColumns?.filter(it => it.length)?.length || 0;
        if (!pkTableColumnsLength) return 'Please select the primary key column(s)';

        if (!fkTableName) return 'Please select a foreign key table';
        const fkTableColumnsLength = fkTableColumns?.filter(it => it.length)?.length || 0;
        if (!fkTableColumnsLength) return 'Please select the foreign key column(s)';

        if (pkTableColumnsLength !== fkTableColumnsLength) return 'Please select the same number of columns';
        return null;
    }

    function SaveSection({saving}: { saving: boolean }) {
        const message = cannotSaveMessage();
        return (
            <>
                {message && <div className={"mb-2"}>{message}</div>}
                <div>
                    <button type={"submit"} className={"btn btn-primary"} disabled={saving || message != null}>Save</button>
                    <NavLink to={"/vfk"} className={"btn btn-default ms-2"}>Cancel</NavLink>
                </div>
            </>
        );
    }

    function setUniqueIndexIfOnlyOne(pkTable: Table) {
        const uniqueIndexes = pkTable.indexes?.filter(it => it.unique);
        if (uniqueIndexes && uniqueIndexes.length) {
            setUniqueIndex(uniqueIndexes[0]);
            setPkTableColumns(uniqueIndexes[0].columns);
        }
    }

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        setSaving(true);
        setError(null);
        axios.post<ForeignKey>('/database/vfks', {
            name: name.length ? name : placeholderName,
            pkTableName: pkTableName,
            pkColumns: pkTableColumns,
            fkTableName: fkTableName,
            fkColumns: fkTableColumns
        }).then(() => {
            navigate("/vfk");
        }).catch(error => {
            setError(error.message);
            setSaving(false);
        });
    }

    if (loading) return <div className="text-center"><i className="fa fa-spinner fa-spin"/> Loading</div>;

    return (
        <>
            <form onSubmit={event => whenSave(event)}>
                <div className={"row"}>
                    <div className={"col-2"}/>
                    <div className={"col-8"}>
                        <label htmlFor="fkName" className="form-label">Name:</label>
                        {editedPkTable == null && (
                            <input className="form-control"
                                   name={"fkName"}
                                   placeholder={placeholderName}
                                   type={"text"}
                                   value={name}
                                   autoComplete={"off"}
                                   onChange={(event) => {
                                       setName(event.target.value)
                                   }}/>
                        )}
                        {editedPkTable != null && <span className="form-control">{name}</span>}

                    </div>
                </div>
                <div className={"row mt-4"}>
                    <div className={"col-2"}/>
                    <div className={"col-4"}>
                        <div>Primary Key Table:</div>
                        <div>
                            {editedPkTable == null && (
                                <SelectTable
                                    setTable={(table) => {
                                        setPkTableName(table.tableName);
                                        let columns = table.columns.map(it => it.name)
                                        setAllPkTableColumns(columns);
                                        setUniqueIndexIfOnlyOne(table);
                                    }}
                                />
                            )}
                            {editedPkTable != null && pkTableName && <span className="form-control">{tableNameToFqName(pkTableName)}</span>}
                        </div>

                        {uniqueIndex == null && (
                            <SelectedColumns allTableColumns={allPkTableColumns}
                                             selectedColumns={pkTableColumns}
                                             setSelectedColumns={setPkTableColumns}
                                             matchColumns={null}
                            />
                        )}

                        {uniqueIndex != null && <IndexColumns columns={uniqueIndex.columns}/>}
                    </div>
                    <div className={"col-4"}>
                        <div>Foreign Key Table:</div>
                        <div>
                            {editedPkTable == null && (
                                <SelectTable
                                    setTable={(table) => {
                                        setFkTableName(table.tableName);
                                        let columns = table.columns.map(it => it.name)
                                        setAllFkTableColumns(columns);
                                    }}
                                />
                            )}
                            {editedPkTable != null && fkTableName && <span className="form-control">{tableNameToFqName(fkTableName)}</span>}
                        </div>
                        {pkTableColumns && <SelectedColumns allTableColumns={allFkTableColumns}
                                                            selectedColumns={fkTableColumns}
                                                            setSelectedColumns={setFkTableColumns}
                                                            matchColumns={pkTableColumns}/>
                        }

                    </div>
                </div>
                <div className={"row"}>
                    <div className={"col-2"}/>
                    <div className={"col-8 mt-4"}>
                        {error && <div className="alert alert-danger start"><i className="fa fa-error"/> {error}</div>}
                        <SaveSection saving={saving}/>
                    </div>
                </div>
            </form>
        </>
    )
}