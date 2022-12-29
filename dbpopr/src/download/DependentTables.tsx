import {SearchTableResult} from "./SelectTable";
import React, {useEffect, useState} from "react";
import axios, {AxiosResponse} from "axios";
import {Dependency} from "./Dependency";

export function DependentTables({rootTable, changeNumber, setChangeNumber, dependency, setDependency}: {
    rootTable: SearchTableResult | null,
    changeNumber: number,
    setChangeNumber: ((i: number) => void),
    dependency: Dependency | null,
    setDependency: ((d: Dependency | null) => void),
}) {
    const [loading, setLoading] = useState<boolean>(false);

    // rootTable:               The root table
    // dependentResults:        Tables that depend on the root table + the checked tables
    // selectedDependentTables: dependent tables with a checkbox checked

    useEffect(() => {
        if (rootTable != null) {
            setLoading(true);
            axios.post<Dependency, AxiosResponse<Dependency>>(`/database/dependencies`, dependency)
                .then((result) => {
                    setLoading(false);
                    setDependency(result.data);
                });
        }
    }, [rootTable, changeNumber]);

    function drawDependencies(dependency: Dependency) {
        return (
            <>
                {dependency.subDependencies != null && dependency.subDependencies.map(it => (
                    <div key={it.constraintName} style={{marginLeft: "1em"}}>
                        {/*Disabled - Lookups*/}
                        {it.mandatory && (
                            <div className="form-check">
                                <input className="form-check-input"
                                       type="checkbox"
                                       id={`dependent-${it.constraintName}`}
                                       checked={true}
                                       disabled/>
                                <label className="form-check-label" htmlFor={`dependent-${it.constraintName}`}>
                                    {it.displayName}
                                </label>
                            </div>
                        )}
                        {/*Enabled - Data*/}
                        {it.mandatory || (
                            <div key={it.constraintName} className="form-check">
                                <input className="form-check-input"
                                       type="checkbox"
                                       id={`dependent-${it.constraintName}`}
                                       checked={it.selected}
                                       disabled={loading}
                                       onChange={e => {
                                           it.selected = e.target.checked;
                                           setChangeNumber(changeNumber + 1);
                                       }}
                                />
                                <label className="form-check-label" htmlFor={`dependent-${it.constraintName}`}>
                                    {it.displayName}
                                </label>
                            </div>
                        )}
                        {drawDependencies(it)}
                    </div>
                ))}
            </>
        )
    }

    return (
        <>
            <div>
                {dependency && drawDependencies(dependency)}
            </div>
        </>
    );
}