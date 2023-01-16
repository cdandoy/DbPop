import React from "react"
import PageHeader from "../../pageheader/PageHeader";
import {Dependency} from "../../../models/Dependency";
import {tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";

export interface DependenciesFilter {
    required: boolean;
    recommended: boolean;
    optional: boolean;
}

export default function DependenciesComponent({
                                                  dependenciesFilter, setDependenciesFilter,
                                                  dependency, recalculateDependencies,
                                                  setPage,
                                              }: {
    dependenciesFilter: DependenciesFilter,
    setDependenciesFilter: ((p: DependenciesFilter) => void),
    dependency: Dependency,
    recalculateDependencies: (() => void),
    setPage: ((p: string) => void),
}) {

    function DependencyComponent({dependency}: {
        dependency: Dependency,
    }) {
        if (!dependenciesFilter.required && dependency.mandatory) return <></>;
        return <>
            <div className="form-check" title={dependency.constraintName || ''}>
                <input className="form-check-input"
                       type="checkbox"
                       id={`dependent-${dependency.constraintName}`}
                       checked={dependency.selected || dependency.mandatory}
                       onChange={e => {
                           dependency.selected = e.target.checked;
                           recalculateDependencies();
                       }}
                       disabled={dependency.mandatory}/>
                <label className="form-check-label"
                       htmlFor={`dependent-${dependency.constraintName}`}
                       style={{opacity: "100%"}}
                >
                    {tableNameToFqName(dependency.tableName)}
                </label>
            </div>
            <div className={"ms-4"}>
                {dependency.subDependencies?.map(subDependency => <DependencyComponent key={subDependency.constraintName} dependency={subDependency}/>)}
            </div>
        </>
    }

    function countSelectedTables(dependency: Dependency) {
        if (!dependency.selected) return 0;
        let count = 1;
        dependency.subDependencies?.forEach(it => count += countSelectedTables(it));
        return count;
    }

    return <>
        <div id={"select-model-dependencies"}>
            <PageHeader title={"Model Download"} subtitle={"Select related tables"}/>
            <div className={"mt-3 mb-3 button-bar"}>
                <div className={"btn-group"}>
                    <button className={"btn btn-primary"} onClick={() => setPage("baseTable")}>
                        <i className={"fa fa-arrow-left"}/>
                        &nbsp;
                        Back
                    </button>
                    <button className={"btn btn-primary"} onClick={() => setPage("dataFilter")}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </div>
            </div>
            <div>{Plural(countSelectedTables(dependency), "table")} selected</div>
            <FilterComponent dependenciesFilter={dependenciesFilter} setDependenciesFilter={setDependenciesFilter}/>
            <div className={"mt-3"}>
                <div>
                    <h4>{tableNameToFqName(dependency.tableName)}</h4>
                    {dependency.subDependencies?.map(subDependency => <DependencyComponent key={subDependency.constraintName} dependency={subDependency}/>)}
                </div>
            </div>
        </div>
    </>
}

function FilterComponent({dependenciesFilter, setDependenciesFilter}: {
    dependenciesFilter: DependenciesFilter,
    setDependenciesFilter: ((p: DependenciesFilter) => void),
}) {
    return (
        <div className={"row"}>
            <div key={"filter-required"} className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={dependenciesFilter.required}
                           onChange={e => setDependenciesFilter({...dependenciesFilter, ...{required: e.target.checked}})}
                           id={"filter-required"}/>
                    <label className="form-check-label" htmlFor="filter-required">
                        Required
                    </label>
                </div>
            </div>
            <div key={"filter-recommended"} className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={dependenciesFilter.recommended}
                           onChange={e => setDependenciesFilter({...dependenciesFilter, ...{recommended: e.target.checked}})}
                           id={"filter-recommended"}/>
                    <label className="form-check-label" htmlFor="filter-recommended">
                        Recommended
                    </label>
                </div>
            </div>
            <div key={"filter-optional"} className={"col-2"}>
                <div className="form-check" style={{paddingTop: "38px"}}>
                    <input className="form-check-input"
                           type="checkbox"
                           checked={dependenciesFilter.optional}
                           onChange={e => setDependenciesFilter({...dependenciesFilter, ...{optional: e.target.checked}})}
                           id={"filter-optional"}/>
                    <label className="form-check-label" htmlFor="filter-optional">
                        Optional
                    </label>
                </div>
            </div>
        </div>
    )
}

