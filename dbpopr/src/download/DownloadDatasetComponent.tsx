import React, {useEffect, useState} from "react";
import {NavLink, useParams} from "react-router-dom";
import {DatasetResponse} from "../models/DatasetResponse";
import axios from "axios";
import {toHumanReadableSize} from "../utils/DbPopUtils";

export default function DownloadDatasetComponent(): JSX.Element {
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
    }, []);

    return (
        <div>
            <nav aria-label="breadcrumb">
                <ol className="breadcrumb">
                    <li className="breadcrumb-item"><NavLink to="/">Home</NavLink></li>
                    <li className="breadcrumb-item active" aria-current="page">Dataset: {datasetName}</li>
                </ol>
            </nav>

            {loaded || <div ><i className="fa fa-spinner fa-spin"/> Loading</div>}
            {loaded && (
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
                    </tbody>
                </table>
            )}
        </div>
    )
}