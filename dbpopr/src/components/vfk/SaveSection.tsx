import {NavLink} from "react-router-dom";
import React from "react";
import {TableName} from "../../models/TableName";

export default function SaveSection({pkTableName, pkTableColumns, fkTableName, fkTableColumns, saving}: {
    pkTableName?: TableName,
    pkTableColumns: string[],
    fkTableName?: TableName,
    fkTableColumns: string[],
    saving: boolean;
}) {
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
