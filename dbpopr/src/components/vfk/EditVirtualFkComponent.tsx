import React, {useEffect, useState} from "react";
import {useNavigate, useParams} from "react-router-dom";
import {tableNameToFqName} from "../../models/TableName";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import SelectedColumns from "./SelectedColumns";
import {getVirtualForeignKey, saveVirtualForeignKey} from "../../api/VirtualForeignKey";
import {getTable} from "../../api/Table";
import SaveSection from "./SaveSection";
import {Table} from "../../models/Table";
import {ForeignKey} from "../../models/ForeignKey";

export default function EditVirtualFkComponent() {
    const params = useParams();
    const paramPkTable = params.pkTable!;
    const paramFkName = params.fkName!;
    const [loading, setLoading] = useState<boolean>(true)
    const [saving, setSaving] = useState<boolean>(false)
    const [error, setError] = useState<string | null>('')
    const [foreignKey, setForeignKey] = useState<ForeignKey | undefined>();
    const [fkTable, setFkTable] = useState<Table | undefined>();
    // Selected FK columns
    const [fkTableColumns, setFkTableColumns] = useState<string[]>([]);
    const navigate = useNavigate();

    useEffect(() => {
        setLoading(true);
        const pkParts = paramPkTable.split('.');
        const pkTableName = {catalog: pkParts[0], schema: pkParts[1], table: pkParts[2],}
        // Get the FK info
        getVirtualForeignKey(pkTableName, paramFkName)
            .then((result) => {
                const foreignKey = result.data;
                setForeignKey(foreignKey);
                setFkTableColumns(foreignKey.fkColumns);
            })
            .finally(() => setLoading(false));
    }, [paramPkTable, paramFkName]);

    useEffect(() => {
        if (foreignKey) {
            setLoading(true);
            getTable(foreignKey.fkTableName)
                .then((result) => setFkTable(result.data))
                .finally(() => setLoading(false));
        }
    }, [foreignKey])

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        if (foreignKey) {
            setSaving(true);
            setError(null);
            saveVirtualForeignKey(paramFkName, foreignKey.pkTableName, foreignKey.pkColumns, foreignKey.fkTableName, fkTableColumns)
                .then(() => navigate("/vfk"))
                .catch(error => {
                    setError(error.response.data?.detail || error.message);
                    setSaving(false);
                });
        }
    }

    return (
        <>
            <LoadingOverlay active={loading}/>
            <PageHeader title={"Virtual Foreign Keys"}/>
            <form onSubmit={event => whenSave(event)}>
                <div className={"row"}>
                    <div className={"col-8"}>
                        <label htmlFor="fkName" className="form-label">Name:</label>
                        <span className="form-control">{paramFkName}</span>
                    </div>
                </div>
                <div className={"row mt-4"}>
                    <div className={"col-4"}>
                        <div>Primary Key Table:</div>
                        <div>
                            <span className="form-control">{foreignKey ? tableNameToFqName(foreignKey.pkTableName) : ''}</span>
                        </div>
                        {foreignKey && foreignKey.pkColumns.map(column => (
                            <div key={column} className={"ms-4 mt-2"}>
                                <span className={"form-control"}>{column}</span>
                            </div>
                        ))}
                    </div>
                    <div className={"col-4"}>
                        <div>Foreign Key Table:</div>
                        <div>
                            <span className="form-control">{foreignKey ? tableNameToFqName(foreignKey.fkTableName) : ''}</span>
                        </div>
                        {foreignKey && fkTable && <SelectedColumns allTableColumns={fkTable.columns}
                                                                   selectedColumns={fkTableColumns}
                                                                   setSelectedColumns={setFkTableColumns}
                                                                   matchColumns={foreignKey.pkColumns}/>
                        }

                    </div>
                </div>
                <div className={"row"}>
                    <div className={"col-8 mt-4"}>
                        {error && <div className="alert alert-danger start"><i className="fa fa-error"/> {error}</div>}
                        <SaveSection pkTableName={foreignKey?.pkTableName}
                                     pkTableColumns={foreignKey?.pkColumns || []}
                                     fkTableName={foreignKey?.fkTableName}
                                     fkTableColumns={fkTableColumns}
                                     saving={saving}/>
                    </div>
                </div>
            </form>
        </>
    )
}