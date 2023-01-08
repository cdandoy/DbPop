import React, {useEffect, useState} from "react";
import {NavLink, useParams} from "react-router-dom";
import {DatasetResponse} from "../models/DatasetResponse";
import axios from "axios";

export default function DatasetDetails() {
    const routeParams = useParams();
    const datasetName = routeParams['dataset']
    const [dataset, setDataset] = useState<DatasetResponse | null>(null);
    const [loaded, setLoaded] = useState<boolean>(false);

    useEffect(() => {
        axios.get<DatasetResponse>(`/datasets/content/${datasetName}`)
            .then((result) => {
                setLoaded(true);
                setDataset(result.data);
            });
    }, [datasetName]);

    return (
        <div>
            <nav aria-label="breadcrumb">
                <ol className="breadcrumb">
                    <li className="breadcrumb-item"><NavLink to="/">Home</NavLink></li>
                    <li className="breadcrumb-item active" aria-current="page">Dataset: {datasetName}</li>
                </ol>
            </nav>

            {loaded || <div><i className="fa fa-spinner fa-spin"/> Loading</div>}
            {loaded && (
                <table className="table table-hover">
                    <thead>
                    <tr>
                        <th>File</th>
                        <th className={"text-end"}>Rows</th>
                        <th className={"text-end"}>Size</th>
                    </tr>
                    </thead>
                    <tbody>
                    {dataset?.files?.map(file => {
                        return (
                            <tr key={file.name}>
                                <td>{file.name}</td>
                                <td className={"text-end"}>{file.rows.toLocaleString()}</td>
                                <td className={"text-end"}>{(file.fileSize / 1024.0).toFixed(2)} Kb</td>
                            </tr>
                        )
                    })}
                    </tbody>
                </table>
            )}
        </div>
    )
}