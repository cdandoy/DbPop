import React from "react";
import {Index} from "../../models/Table";

export default function IndexColumns({uniqueIndex}: { uniqueIndex: Index | null }) {
    if (!uniqueIndex) return <></>
    return <>
        {uniqueIndex.columns.map(column => (
            <div key={column} className={"ms-4 mt-2"}>
                <span className={"form-control"}>{column}</span>
            </div>
        ))}
    </>
}
