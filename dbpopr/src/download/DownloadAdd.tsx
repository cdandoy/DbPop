import React, {useState} from "react";
import {NavLink, useParams} from "react-router-dom";
import {SearchTableResult, SelectTable} from "./SelectTable";
import {DependentTables} from "./DependentTables";

export default function DownloadAdd() {
    const routeParams = useParams();
    const datasetName = routeParams['dataset']
    const [tableSelections, setTableSelections] = useState<SearchTableResult[]>([]);
    const [queryValues, setQueryValues] = useState<any>({})

    function whenSearchSubmitted(e: any) {
        e.preventDefault();
        console.log('HELO')
    }

    return (
        <div>
            <nav aria-label="breadcrumb">
                <ol className="breadcrumb">
                    <li className="breadcrumb-item"><NavLink to="/">Home</NavLink></li>
                    <li className="breadcrumb-item active" aria-current="page">Add to: <strong>{datasetName}</strong></li>
                </ol>
            </nav>

            <div className="mb-3">
                <label htmlFor="table-name" className="form-label">Table Name:</label>
                <SelectTable setTableSelections={setTableSelections}/>
            </div>

            <div className="mb-3">
                <label htmlFor="table-name" className="form-label">Dependent:</label>
                <DependentTables tableSelections={tableSelections}/>
            </div>

            {tableSelections.length > 0 &&
                <form onSubmit={whenSearchSubmitted}>
                    <table className="table table-hover">
                        {tableSelections.map(tableSelection => {
                            const searchables = tableSelection.searches.map(search => {
                                return search.columns[0]
                            });
                            return (
                                <thead key={1}>
                                <tr>
                                    {tableSelection.columns.map(column => {
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
                            )
                        })}
                        {tableSelections.map(tableSelection => (
                            <thead key={1}>
                            <tr>
                                {tableSelection.columns.map((column) => {
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
                        ))}
                    </table>
                    {Object.keys(queryValues).map(column => <div key={column}>{column} = {queryValues[column]}</div>)}
                </form>
            }
        </div>
    )
}