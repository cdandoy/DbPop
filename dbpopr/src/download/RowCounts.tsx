import React, {useEffect, useState} from "react";
import {Dependency} from "./Dependency";
import axios, {AxiosResponse} from "axios";

export default function RowCounts({changeNumber, dataset, dependency, queryValues}: {
    dataset: string,
    changeNumber: number,
    dependency: Dependency,
    queryValues: any
}) {
    const [loading, setLoading] = useState<boolean>(false);
    const [loaded, setLoaded] = useState<number>(0);

    useEffect(() => {
        if (dependency != null) {
            setLoading(true);
            setLoaded(loaded + 1);

            for (let [key, value] of Object.entries(queryValues)) {
                if (!value) {
                    delete queryValues[key];
                }
            }

            axios.post<Dependency, AxiosResponse<Dependency>>(`/download`, {
                dataset,
                dependency,
                queryValues,
                dryRun: true
            })
                .then((result) => {
                    setLoading(false);
                });
        }
    }, [changeNumber]);

    return (
        <>
            <div>Change number: {changeNumber}</div>
            <div>Loaded: {loaded}</div>
        </>
    );
}