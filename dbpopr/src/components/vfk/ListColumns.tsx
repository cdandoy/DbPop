import React from "react";

export default function ListColumns({columns}: {
    columns: string[]
}) {
    return <>
        {
            columns.map(column => (
                <div key={column} className={"ms-4 mt-2"}>
                    <span className={"form-control"}>{column}</span>
                </div>
            ))
        }
    </>
}