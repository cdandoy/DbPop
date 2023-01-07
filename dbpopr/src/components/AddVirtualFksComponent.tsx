import React, {useEffect, useState} from "react";
import {SearchTableResult, SelectTable} from "./SelectTable";
import axios from "axios";
import {ForeignKey} from "../models/ForeignKey";
import {NavLink, useNavigate} from "react-router-dom";

function SelectedColumns({searchTableResult, selectedColumns, setSelectedColumns}: {
    searchTableResult: SearchTableResult | null,
    selectedColumns: string[],
    setSelectedColumns: (s: string[]) => void,
}) {
    if (!searchTableResult) return <></>;

    selectedColumns = selectedColumns.filter(it => it.length).concat(['']);
    const columns = [''].concat(searchTableResult.columns);

    return (
        <>
            {selectedColumns.map((selectedColumn, i) => {
                return (
                    <div key={i} className={"ms-4 mt-2"}>
                        <select key={selectedColumn}
                                className="form-select"
                                aria-label="Column"
                                defaultValue={selectedColumn}
                                onChange={event => {
                                    selectedColumns[i] = event.target.value;
                                    selectedColumns = selectedColumns.filter(it => it.length);
                                    setSelectedColumns(selectedColumns);
                                }}>
                            {columns
                                .filter(column => selectedColumns.slice(0, i).indexOf(column) == -1) // Do not include already selected columns
                                .map(column => {
                                    return (
                                        <option key={column} value={column}>{column}</option>
                                    )
                                })}
                        </select>
                    </div>
                );
            })}
        </>
    )
}

export default function AddVirtualFksComponent() {
    const [saving, setSaving] = useState<boolean>(false)
    const [error, setError] = useState<string | null>('')
    const [name, setName] = useState<string>('');
    const [placeholderName, setPlaceholderName] = useState<string>('');
    // Selected PK table
    const [pkSearchTableResult, setPkSearchTableResult] = useState<SearchTableResult | null>(null);
    // Selected PK columns
    const [pkTableColumns, setPkTableColumns] = useState<string[]>([]);
    // Selected FK table
    const [fkSearchTableResult, setFkSearchTableResult] = useState<SearchTableResult | null>(null);
    // Selected FK columns
    const [fkTableColumns, setFkTableColumns] = useState<string[]>([]);
    const navigate = useNavigate();

    useEffect(() => {
        if (pkSearchTableResult && fkSearchTableResult) {
            setPlaceholderName(pkSearchTableResult.tableName.table + '_' + fkSearchTableResult.tableName.table + '_fk')
        }
    }, [pkSearchTableResult, fkSearchTableResult])

    function cannotSaveMessage(): string | null {
        if (!pkSearchTableResult) return 'Please select a primary key table';
        if (!fkSearchTableResult) return 'Please select a foreign key table';
        if (!pkTableColumns.length) return 'Please select the primary key column(s)';
        if (!fkTableColumns.length) return 'Please select the foreign key column(s)';
        if (pkTableColumns.length != fkTableColumns.length) return 'Please select the same number of columns';
        return null;
    }

    function SaveSection({saving}: { saving: boolean }) {
        const message = cannotSaveMessage();
        return (
            <>
                {message && <div className={"mb-2"}>{message}</div>}
                <button type={"submit"} className={"btn btn-primary"} disabled={saving || message != null}>
                    Save
                </button>
            </>
        );
    }

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        setSaving(true);
        setError(null);
        axios.post<ForeignKey>('/database/vfks', {
            name: name.length ? name : placeholderName,
            pkTableName: pkSearchTableResult!.tableName,
            pkColumns: pkTableColumns,
            fkTableName: fkSearchTableResult!.tableName,
            fkColumns: fkTableColumns
        }).then(() => {
            navigate("/vfk");
        }).catch(error => {
            setError(error.message);
            setSaving(false);
        });
    }

    return (
        <>
            <form onSubmit={event => whenSave(event)}>
                <div className={"row"}>
                    <div className={"col-2"}/>
                    <div className={"col-8 mb-3"}>
                        <NavLink to={"/vfk"}>
                            <i className={"fa fa-arrow-left"}/>
                            Back
                        </NavLink>
                    </div>
                </div>
                <div className={"row"}>
                    <div className={"col-2"}/>
                    <div className={"col-8"}>
                        <label htmlFor="fkName" className="form-label">Name:</label>
                        <input className="form-control"
                               name={"fkName"}
                               placeholder={placeholderName}
                               type={"text"}
                               value={name}
                               onChange={(event) => {
                                   setName(event.target.value)
                               }}/>
                    </div>
                </div>
                <div className={"row mt-4"}>
                    <div className={"col-2"}/>
                    <div className={"col-4"}>
                        <div>Primary Key:</div>
                        <div>
                            <SelectTable setTableSelection={setPkSearchTableResult}/>
                        </div>
                        <SelectedColumns searchTableResult={pkSearchTableResult}
                                         selectedColumns={pkTableColumns}
                                         setSelectedColumns={setPkTableColumns}
                        />
                    </div>
                    <div className={"col-4"}>
                        <div>Foreign Key:</div>
                        <div>
                            <SelectTable setTableSelection={setFkSearchTableResult}/>
                        </div>
                        <SelectedColumns searchTableResult={fkSearchTableResult}
                                         selectedColumns={fkTableColumns}
                                         setSelectedColumns={setFkTableColumns}
                        />
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