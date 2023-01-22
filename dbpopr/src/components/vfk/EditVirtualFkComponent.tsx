import React, {useEffect, useState} from "react";
import {useNavigate, useParams} from "react-router-dom";
import {TableName, tableNameToFqName} from "../../models/TableName";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import SelectedColumns from "./SelectedColumns";
import {getVirtualForeignKey, saveVirtualForeignKey} from "../../api/VirtualForeignKey";
import {getTable} from "../../api/Table";
import SaveSection from "./SaveSection";

export default function EditVirtualFkComponent() {
    const params = useParams();
    const editedFkName = params.fkName;
    const [loading, setLoading] = useState<boolean>(true)
    const [saving, setSaving] = useState<boolean>(false)
    const [error, setError] = useState<string | null>('')
    const [name, setName] = useState<string>('');
    // Selected PK table
    const pkTableName = getTableName(params.pkTable!);
    // Selected PK columns
    const [pkTableColumns, setPkTableColumns] = useState<string[]>([]);

    // Selected FK table
    const [fkTableName, setFkTableName] = useState<TableName | undefined>();
    // All columns of the selected FK table
    const [allFkTableColumns, setAllFkTableColumns] = useState<string[] | null>(null);
    // Selected FK columns
    const [fkTableColumns, setFkTableColumns] = useState<string[]>([]);
    const navigate = useNavigate();

    function getTableName(tableString: string): TableName {
        let pkParts = tableString.split('.');
        return {catalog: pkParts[0], schema: pkParts[1], table: pkParts[2],}
    }

    useEffect(() => {
        if (!(pkTableName && editedFkName)) {
            setLoading(false);
            return;
        }
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
                                setName(fk.name);
                                setFkTableName(fkTableName);
                                setPkTableColumns(fk.pkColumns);
                                setFkTableColumns(fk.fkColumns);
                                setAllFkTableColumns(fkTable.columns.map(it => it.name));
                                setLoading(false);
                            }
                        })
                }
            })
    }, []);

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        setSaving(true);
        setError(null);
        saveVirtualForeignKey(name, pkTableName!, pkTableColumns, fkTableName!, fkTableColumns)
            .then(() => navigate("/vfk"))
            .catch(error => {
                setError(error.message);
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
                        <span className="form-control">{name}</span>
                    </div>
                </div>
                <div className={"row mt-4"}>
                    <div className={"col-4"}>
                        <div>Primary Key Table:</div>
                        <div>
                            <span className="form-control">{tableNameToFqName(pkTableName)}</span>
                        </div>
                        {pkTableColumns.map(column => (
                            <div key={column} className={"ms-4 mt-2"}>
                                <span className={"form-control"}>{column}</span>
                            </div>
                        ))}
                    </div>
                    <div className={"col-4"}>
                        <div>Foreign Key Table:</div>
                        <div>
                            <span className="form-control">{fkTableName ? tableNameToFqName(fkTableName) : ''}</span>
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