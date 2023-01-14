import React from "react";

export function FilterComponent({nameFilter, setNameFilter, downloaded, setDownloaded, empty, setEmpty, dependencies, setDependencies}: {
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
            <div className="mb-3 col-3">
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
                           checked={empty}
                           onChange={e => setEmpty(e.target.checked)}
                           id={"filter-empty"}/>
                    <label className="form-check-label" htmlFor="filter-empty">
                        Empty
                    </label>
                </div>
            </div>
            <div className={"col-2"}>
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
            <div className={"col-2"}>
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