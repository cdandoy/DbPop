import React from "react";
import {SearchTableResult} from "./SelectTable";

export default function FilterForm({rootTable, queryValues, setQueryValues, whenSearchSubmitted}: {
    rootTable: SearchTableResult | null
    queryValues: any,
    setQueryValues: ((s: any) => void),
    whenSearchSubmitted: (() => void),
}) {
    if (rootTable == null) return <></>;
    const searchables = rootTable.searches.map(search => {
        return search.columns[0]
    });
    return (
        <>
            <form onSubmit={e => {
                e.preventDefault();
                whenSearchSubmitted()
            }}>
                <div className={"row"}>
                    {rootTable.columns.map(column => (
                        <div key={column} className="col-3 mb-3">
                            <label htmlFor={`filter-${column}`} className="form-label">
                                {column}
                                {searchables.includes(column) && (
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
                                       clone[column] = e.target.value;
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