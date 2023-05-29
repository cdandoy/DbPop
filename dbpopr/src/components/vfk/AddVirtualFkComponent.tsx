import React, {useEffect, useState} from "react";
import {SelectTable} from "../SelectTable";
import {useNavigate} from "react-router-dom";
import {Index, Table} from "../../models/Table";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import SelectedColumns from "./SelectedColumns";
import {saveVirtualForeignKey} from "../../api/VirtualForeignKey";
import SaveSection from "./SaveSection";
import ListColumns from "./ListColumns";

export default function AddVirtualFkComponent() {
    const [saving, setSaving] = useState<boolean>(false)
    const [error, setError] = useState<string | null>('')
    const [name, setName] = useState<string>('');
    const [placeholderName, setPlaceholderName] = useState<string>('');
    // Selected PK table
    const [pkTable, setPkTable] = useState<Table | undefined>()
    // Selected PK columns
    const [pkTableColumns, setPkTableColumns] = useState<string[]>([]);
    const [uniqueIndex, setUniqueIndex] = useState<Index | null>(null);

    // Selected FK table
    const [fkTable, setFkTable] = useState<Table | undefined>();
    // Selected FK columns
    const [fkTableColumns, setFkTableColumns] = useState<string[]>([]);
    const navigate = useNavigate();

    useEffect(() => {
        if (pkTable && fkTable) {
            setPlaceholderName(pkTable.tableName.table + '_' + fkTable.tableName.table + '_fk')
        }
    }, [pkTable, fkTable]);

    useEffect(() => {
        if (pkTable) {
            const uniqueIndexes = pkTable.indexes?.filter(it => it.unique);
            if (uniqueIndexes && uniqueIndexes.length) {
                const uniqueIndex = uniqueIndexes[0];
                setUniqueIndex(uniqueIndex);
                setPkTableColumns(uniqueIndex.columns);
            }
        }
    }, [pkTable]);

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        if (pkTable && fkTable) {
            setSaving(true);
            setError(null);
            saveVirtualForeignKey(
                name.length ? name : placeholderName,
                pkTable.tableName,
                pkTableColumns,
                fkTable.tableName,
                fkTableColumns
            )
                .then(() => navigate("/vfk"))
                .catch(error => {
                    setError(error.response.data?.detail || error.message);
                    setSaving(false);
                });
        }
    }

    return (
        <div className={"container"}>
            <LoadingOverlay active={saving}/>
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
                            <SelectTable setTable={(table) => setPkTable(table)}/>
                        </div>

                        {uniqueIndex == null && (
                            <SelectedColumns allTableColumns={pkTable?.columns || []}
                                             selectedColumns={pkTableColumns}
                                             setSelectedColumns={setPkTableColumns}/>
                        )}

                        {uniqueIndex != null && (
                            <ListColumns columns={uniqueIndex.columns}/>
                        )}
                    </div>
                    <div className={"col-4"}>
                        <div>Foreign Key Table:</div>
                        <div>
                            <SelectTable setTable={(table) => setFkTable(table)}/>
                        </div>
                        {pkTableColumns && <SelectedColumns allTableColumns={fkTable?.columns || []}
                                                            selectedColumns={fkTableColumns}
                                                            setSelectedColumns={setFkTableColumns}
                                                            matchColumns={pkTableColumns}/>
                        }
                    </div>
                </div>
                <div className={"row"}>
                    <div className={"col-8 mt-4"}>
                        {error && <div className="alert alert-danger start"><i className="fa fa-error"/> {error}</div>}
                        <SaveSection pkTableName={pkTable?.tableName}
                                     pkTableColumns={pkTableColumns}
                                     fkTableName={fkTable?.tableName}
                                     fkTableColumns={fkTableColumns}
                                     saving={saving}/>
                    </div>
                </div>
            </form>
        </div>
    )
}