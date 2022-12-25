import React, {useEffect, useState} from "react";
import {NavLink, useParams} from "react-router-dom";
import {DatasetResponse} from "../models/DatasetResponse";
import axios from "axios";
import {toHumanReadableSize} from "../utils/DbPopUtils";

export default function DownloadDatasetComponent(): JSX.Element {
    const routeParams = useParams();
    const datasetName = routeParams['dataset']
    const [dataset, setDataset] = useState<DatasetResponse | null>(null);

    useEffect(() => {
        axios.get<DatasetResponse>(`/datasets/content/${datasetName}`)
            .then((result) => setDataset(result.data));
    }, []);

    return (
        <div>
            <nav aria-label="breadcrumb">
                <ol className="breadcrumb">
                    <li className="breadcrumb-item"><NavLink to="/">Home</NavLink></li>
                    <li className="breadcrumb-item active" aria-current="page">Dataset: {datasetName}</li>
                </ol>
            </nav>

            <div className="card">
                <div className="card-body">
                    This dataset contains .
                </div>
            </div>

            <table className="table table-hover">
                <thead>
                <tr>
                    <th>File</th>
                    <th>Rows</th>
                    <th>Size</th>
                </tr>
                </thead>
                <tbody>
                {dataset?.files?.map(file => {
                    let readableSize = toHumanReadableSize(file.fileSize);
                    return (
                        <tr>
                            <td>{file.name}</td>
                            <td>{file.rows}</td>
                            <td>{readableSize.text}</td>
                        </tr>
                    )
                })}
                {dataset?.files?.map(file => (
                    <tr>
                        <td>{file.name}</td>
                        <td>{file.rows}</td>
                        <td>{file.fileSize}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    )
}