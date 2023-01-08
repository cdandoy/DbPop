import React from "react";
import {Table} from "../models/Table";

export default function FilterForm({rootTable, queryValues, setQueryValues, whenSearchSubmitted}: {
    rootTable: Table,
    queryValues: any,
    setQueryValues: ((s: any) => void),
    whenSearchSubmitted: (() => void),
}) {
    if (rootTable == null) return <></>;
    const searchables: string[] = rootTable?.indexes?.map(it => it.columns)?.flat() || [];

    return (
        <>
            <form onSubmit={e => {
                e.preventDefault();
                whenSearchSubmitted()
            }}>
                <div className={"row"}>
                    {rootTable.columns.map(column => (
                        <div key={column.name} className="col-3 mb-3">
                            <label htmlFor={`filter-${column.name}`} className="form-label">
                                {column.name}
                                {searchables.includes(column.name) && (
                                    <>
                                        &nbsp;
                                        <i className={"fa fa-key fa-xs"} style={{opacity: .6}}/>
                                    </>
                                )}
                            </label>
                            <input type="text"
                                   className="form-control"
                                   id={`filter-${column}`}
                                   onChange={e => {
                                       const clone = structuredClone(queryValues);
                                       clone[column.name] = e.target.value;
                                       setQueryValues(clone);
                                   }}/>
                        </div>
                    ))}
                </div>
                <div>
                    <button type='submit' className={"btn btn-primary"}>
                        <i className={"fa fa-search"}/> Search
                    </button>
                </div>
            </form>
        </>
    )
}