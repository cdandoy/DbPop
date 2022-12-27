import {SearchTableResult} from "./SelectTable";
import React, {useEffect, useState} from "react";
import axios from "axios";
import {TableName} from "../models/TableName";

interface DependentResult {
    displayName: string;
    tableName: TableName;
    optional: boolean;
}

export function DependentTables({rootTable, selectedDependentTables, setSelectedDependentTables}: {
    rootTable: SearchTableResult | null,
    selectedDependentTables: string[],
    setSelectedDependentTables: ((s: string[]) => void),
}) {
    const [dependentResults, setDependentResults] = useState<DependentResult[]>([]);
    const [loading, setLoading] = useState<boolean>(false);

    // rootTable:               The root table
    // dependentResults:        Tables that depend on the root table + the checked tables
    // selectedDependentTables: dependent tables with a checkbox checked

    useEffect(() => {
        if (rootTable != null) {
            setLoading(true);
            let queryTableNames = dependentResults
                .filter(it => selectedDependentTables.includes(it.displayName))
                .map(it => it.tableName);
            queryTableNames.splice(0, 0, rootTable.tableName)
            axios.post<DependentResult[]>(`/database/dependents`, queryTableNames)
                .then((result) => {
                    setLoading(false);
                    let filtered = result.data.filter(it => it.displayName !== rootTable.displayName);
                    setDependentResults(filtered);
                });
        }
    }, [rootTable, selectedDependentTables]);

    return (
        <>
            <div>
                {dependentResults
                    .sort((a, b) => (b.optional ? 1 : 0) - (a.optional ? 1 : 0))
                    .map(dependentResult => (
                        <div key={dependentResult.displayName}>
                            {/*Disabled - Lookups*/}
                            {dependentResult.optional || (
                                <div className="form-check">
                                    <input className="form-check-input"
                                           type="checkbox"
                                           id={`dependent-${dependentResult.displayName}`}
                                           checked={true}
                                           disabled/>
                                    <label className="form-check-label" htmlFor={`dependent-${dependentResult.displayName}`}>
                                        {dependentResult.displayName}
                                    </label>
                                </div>
                            )}
                            {/*Enabled - Data*/}
                            {dependentResult.optional && (
                                <div key={dependentResult.displayName} className="form-check">
                                    <input className="form-check-input"
                                           type="checkbox"
                                           id={`dependent-${dependentResult.displayName}`}
                                           checked={selectedDependentTables.includes(dependentResult.displayName)}
                                           disabled={loading}
                                           onChange={e => {
                                               if (e.target.checked) {
                                                   setSelectedDependentTables([...selectedDependentTables, dependentResult.displayName]);
                                               } else {
                                                   setSelectedDependentTables(selectedDependentTables.filter(it => it !== dependentResult.displayName));
                                               }
                                           }}
                                    />
                                    <label className="form-check-label" htmlFor={`dependent-${dependentResult.displayName}`}>
                                        {dependentResult.displayName}
                                    </label>
                                </div>
                            )}
                        </div>
                    ))}
            </div>
        </>
    )
}