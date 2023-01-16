import React from "react";

export default function ModelFilterComponent({
                                                 schemas,
                                                 schema, setSchema,
                                                 nameFilter, setNameFilter,
                                                 dependenciesFilter, setDependenciesFilter
                                             }: {
    schemas: string[];
    schema: string;
    setSchema: ((s: string) => void)
    nameFilter: string;
    setNameFilter: ((s: string) => void),
    dependenciesFilter: boolean;
    setDependenciesFilter: ((s: boolean) => void)
}) {
    return <>
        <div className={"row"}>
            <div className="col-3">
                <label htmlFor="filterSchema" className="form-label">Schema:</label>
                <select className="form-select" defaultValue={schema} onChange={e => setSchema(e.target.value)}>
                    {schemas.map(schema => <option key={schema} value={schema}>{schema}</option>)}
                </select>
            </div>
            <div className="col-3">
                <label htmlFor="filterTable" className="form-label">Filter:</label>
                <input type="text"
                       className="form-control"
                       id="filterTable"
                       placeholder="Filter table"
                       autoFocus={true}
                       autoComplete={"off"}
                       spellCheck={false}
                       value={nameFilter}
                       onChange={(e) => setNameFilter(e.target.value)}
                />
            </div>
            <div className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={dependenciesFilter}
                           onChange={e => setDependenciesFilter(e.target.checked)}
                           id={"filter-dependencies"}/>
                    <label className="form-check-label" htmlFor="filter-dependencies">
                        Dependencies
                    </label>
                </div>
            </div>
        </div>
    </>
}