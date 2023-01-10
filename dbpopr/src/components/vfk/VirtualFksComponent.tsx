import React, {useEffect, useState} from "react";
import axios from "axios";
import {tableNameToFqName} from "../../models/TableName";
import {NavLink} from "react-router-dom";
import {ForeignKey} from "../../models/ForeignKey";

export default function VirtualFksComponent() {
    const [foreignKeys, setForeignKeys] = useState<ForeignKey[]>([]);
    const [loading, setLoading] = useState<boolean>(false);

    useEffect(() => {
        loadForeignKeys();
    }, []);

    function loadForeignKeys() {
        setLoading(true);
        axios.get<ForeignKey[]>(`/database/vfks`)
            .then((result) => {
                setLoading(false);
                setForeignKeys(result.data);
            });
    }

    function whenDeleteForeignKey(foreignKey: ForeignKey) {
        axios.delete<ForeignKey>('/database/vfks', {
            data: foreignKey
        }).then(() => {
            loadForeignKeys();
        });
    }

    function getVfkLink(foreignKey: ForeignKey):string {
        let tn = foreignKey.pkTableName;
        return `/vfk/${tn.catalog||''}.${tn.schema||''}.${tn.table}/${foreignKey.name}`
    }

    if (loading) return <div className="m-3"><i className="fa fa-fw fa-spinner fa-spin"></i> Loading...</div>;

    return (
        <>
            <div className={"mb-3"}>
                <NavLink to={"/"}>
                    <i className={"fa fa-arrow-left"}/>
                    Back
                </NavLink>
            </div>

            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Name</th>
                    <th>PK</th>
                    <th>FK</th>
                    <th style={{width: "1px"}}></th>
                </tr>
                </thead>
                <tbody>
                {foreignKeys.length === 0 && <>
                    <tr>
                        <td colSpan={3} className={"text-center"}>No Data</td>
                    </tr>
                </>}
                {foreignKeys.map(foreignKey => (
                    <tr key={tableNameToFqName(foreignKey.pkTableName) + '.' + foreignKey.name}>
                        <td>
                            <NavLink to={getVfkLink(foreignKey)}>
                                {foreignKey.name}
                            </NavLink>
                        </td>
                        <td>{tableNameToFqName(foreignKey.pkTableName)}</td>
                        <td>{tableNameToFqName(foreignKey.fkTableName)}</td>
                        <td>
                            <button className={"btn btn-sm"} onClick={() => whenDeleteForeignKey(foreignKey)}>
                                <i style={{color: "red"}} className={"fa fa-trash-can"}/>
                            </button>
                        </td>
                    </tr>
                ))}
                </tbody>
            </table>
            <div className={"text-end"}>
                <NavLink className={"btn btn-sm btn-primary"} to={"/vfk/add/"}>
                    <i className={"fa fa-plus"}/>
                    Add Foreign Key
                </NavLink>
            </div>
        </>
    )
}