import React from "react";

export function FilterComponent({schemas, schema, setSchema, nameFilter, setNameFilter, downloaded, setDownloaded, empty, setEmpty, dependencies, setDependencies}: {
    schemas: string[];
    schema: string;
    setSchema: ((s: string) => void)
    nameFilter: string;
    setNameFilter: ((s: string) => void)
    downloaded: boolean;
    setDownloaded: ((s: boolean) => void)
    empty: boolean;
    setEmpty: ((s: boolean) => void)
    dependencies: boolean;
    setDependencies: ((s: boolean) => void)
}) {
    return (
        <div className={"row"}>
            <div key={"filterSchema"} className="col-3">
                <label htmlFor="filterSchema" className="form-label">Schema:</label>
                <select className="form-select" defaultValue={schema} onChange={e => setSchema(e.target.value)}>
                    {schemas.map(schema => <option key={schema} value={schema}>{schema}</option>)}
                </select>
            </div>
            <div key={"filterTable"} className="col-3">
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
            <div key={"filter-empty"} className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={empty}
                           onChange={e => setEmpty(e.target.checked)}
                           id={"filter-empty"}/>
                    <label className="form-check-label" htmlFor="filter-empty">
                        Empty
                    </label>
                </div>
            </div>
            <div key={"filter-downloaded"} className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={downloaded}
                           onChange={e => setDownloaded(e.target.checked)}
                           id={"filter-downloaded"}/>
                    <label className="form-check-label" htmlFor="filter-downloaded">
                        Downloaded
                    </label>
                </div>
            </div>
            <div key={"filter-dependencies"} className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={dependencies}
                           onChange={e => setDependencies(e.target.checked)}
                           id={"filter-dependencies"}/>
                    <label className="form-check-label" htmlFor="filter-dependencies">
                        Dependencies
                    </label>
                </div>
            </div>
        </div>
    )
}