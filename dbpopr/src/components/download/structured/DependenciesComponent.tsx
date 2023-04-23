import React from "react"
import PageHeader from "../../pageheader/PageHeader";
import {Dependency, searchDependency} from "../../../models/Dependency";
import {tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";
import structured_download from "../structured_download.png";

export interface DependenciesFilter {
    required: boolean;
    recommended: boolean;
    optional: boolean;
}

function DependencyComponent({dependency, required, setChecked}: {
    dependency: Dependency,
    required: boolean,
    setChecked: ((d: Dependency, c: boolean) => void)
}) {
    if (!required && dependency.mandatory) return <></>;
    return <>
        <div className="form-check" title={dependency.constraintName || ''}>
            <input className="form-check-input"
                   type="checkbox"
                   id={`dependent-${dependency.constraintName}`}
                   checked={dependency.selected || dependency.mandatory}
                   onChange={e => {
                       setChecked(dependency, e.target.checked);
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
            {dependency.subDependencies?.map(subDependency => (
                <DependencyComponent key={subDependency.constraintName}
                                     dependency={subDependency}
                                     required={required}
                                     setChecked={setChecked}
                />
            ))}
        </div>
    </>
}

export default function DependenciesComponent({
                                                  dependenciesFilter, setDependenciesFilter,
                                                  dependency, setDependency,
                                                  onBack, onNext,
                                              }: {
    dependenciesFilter: DependenciesFilter,
    setDependenciesFilter: ((p: DependenciesFilter) => void),
    dependency: Dependency,
    setDependency: ((p: Dependency) => void),
    onBack: (() => void),
    onNext: (() => void),
}) {

    function countSelectedTables(dependency: Dependency) {
        if (!dependency.selected) return 0;
        let count = 1;
        dependency.subDependencies?.forEach(it => count += countSelectedTables(it));
        return count;
    }

    function onDependencyChecked(d: Dependency, c: boolean) {
        const clone = structuredClone(dependency);
        const updatedDependency = searchDependency(clone, d);
        if (!updatedDependency) return;
        updatedDependency.selected = c;
        setDependency(clone);
    }

    return <>
        <div id={"structured-select-dependencies"}>
            <div className={"mt-3 mb-3 button-bar"}>
                <div className={"btn-group"}>
                    <button className={"btn btn-primary"} onClick={onBack}>
                        <i className={"fa fa-arrow-left"}/>
                        &nbsp;
                        Back
                    </button>
                    <button className={"btn btn-primary"} onClick={onNext}>
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
                    {dependency.subDependencies?.map(subDependency => (
                        <DependencyComponent key={subDependency.constraintName}
                                             dependency={subDependency}
                                             required={dependenciesFilter.required}
                                             setChecked={onDependencyChecked}
                        />
                    ))}
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

