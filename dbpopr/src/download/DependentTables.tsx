import {SearchTableResult} from "./SelectTable";
import React, {useEffect, useState} from "react";
import axios from "axios";
import {TableName} from "../models/TableName";

interface DependentResult {
    displayName: string;
    tableName: TableName;
    optional: boolean;
}

export function DependentTables({tableSelections}: { tableSelections: SearchTableResult[] }) {
    const [dependentResults, setDependentResults] = useState<DependentResult[]>([]);
    const [loading, setLoading] = useState<boolean>(false);

    useEffect(() => {
        if (tableSelections.length > 0) {
            setLoading(true);
            let tableName = tableSelections[0].tableName;
            axios.get<DependentResult[]>(`/database/dependents`, {
                params: {
                    catalog: tableName.catalog,
                    schema: tableName.schema,
                    table: tableName.table,
                }
            })
                .then((result) => {
                    setLoading(false);
                    setDependentResults(result.data);
                });
        }
    }, [tableSelections]);

    return (
        <>
            {loading || (
                <div>
                    {dependentResults.map(dependentResult => (
                        <div key={dependentResult.displayName}>
                            {dependentResult.optional || (
                                <div className="form-check">
                                    <input className="form-check-input" type="checkbox" value="" id="flexCheckIndeterminateDisabled" disabled/>
                                    <label className="form-check-label" htmlFor="flexCheckIndeterminateDisabled">
                                        {dependentResult.displayName}
                                    </label>
                                </div>
                            )}
                            {dependentResult.optional && (
                                <div key={dependentResult.displayName} className="form-check">
                                    <input className="form-check-input" type="checkbox" value="" id="flexCheckDefault"/>
                                    <label className="form-check-label" htmlFor="flexCheckDefault">
                                        {dependentResult.displayName}
                                    </label>
                                </div>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </>
    )
}