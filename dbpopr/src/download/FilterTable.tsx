import React from "react";
import {SearchTableResult} from "./SelectTable";

export default function FilterTable({rootTable, queryValues, setQueryValues, whenSearchSubmitted}: {
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
            <form onSubmit={e=>{e.preventDefault();whenSearchSubmitted()}}>
                <table className="table table-hover">
                    {/*Column Names*/}
                    <thead key={1}>
                    <tr>
                        {rootTable.columns.map(column => {
                            return (
                                <th key={column}>
                                    {column}
                                    {searchables.includes(column) && (
                                        <>
                                            &nbsp;
                                            <i className={"fa fa-key fa-xs"} style={{opacity: .6}}/>
                                        </>
                                    )}
                                </th>
                            )
                        })}
                        <th></th>
                    </tr>
                    </thead>
                    {/*Filters*/}
                    <thead key={1}>
                    <tr>
                        {rootTable.columns.map((column) => {
                            return (
                                <td key={column}>
                                    <input type={"text"}
                                           style={{width: "100%"}}
                                           onChange={e => {
                                               const clone = structuredClone(queryValues);
                                               clone[column] = e.target.value;
                                               setQueryValues(clone);
                                           }}
                                    />
                                </td>
                            )
                        })}
                        <td>
                            <button type='submit' className={"btn btn-sm btn-primary"}>
                                <i className={"fa fa-search"}/>
                            </button>
                        </td>
                    </tr>
                    </thead>
                </table>
                {/*Debug query*/}
                {Object.keys(queryValues).map(column => <div key={column}>{column} = {queryValues[column]}</div>)}
            </form>
        </>
    )
}