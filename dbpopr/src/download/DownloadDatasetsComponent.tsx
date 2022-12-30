import React, {useEffect, useState} from "react";
import axios from "axios";
import {DatasetResponse} from "../models/DatasetResponse";
import {toHumanReadableSize} from "../utils/DbPopUtils";
import {NavLink} from "react-router-dom";

function DatasetComponent({dataset}: { dataset: DatasetResponse }) {
    const files = dataset.files;
    let size = 0;
    let rows = 0;
    for (const file of files) {
        size += file.fileSize;
        rows += file.rows;
    }
    let readableSize = toHumanReadableSize(size);

    return (
        <div className="card m-3">
            <div className="card-body">
                <h5 className="card-title">{dataset.name}</h5>
                <h6 className="card-subtitle mb-2 text-muted">{files.length} files, {readableSize.text}</h6>
                <div className="text-end">
                    <NavLink to={`dataset/${dataset.name}`} className="card-link">Details</NavLink>
                    <NavLink to={`add/${dataset.name}`} className="card-link">Add Data</NavLink>
                </div>
            </div>
        </div>
    )
}

export default function DownloadDatasetsComponent() {
    const [loaded, setLoaded] = useState<boolean>(false);
    const [datasets, setDatasets] = useState<DatasetResponse[]>([]);
    useEffect(() => {
        axios.get<DatasetResponse[]>("/datasets/content")
            .then((result) => {
                setLoaded(true);
                setDatasets(result.data);
            });
    }, []);
    return (
        <div className="card datasets">
            <div className="card-body">
                <h5 className="card-title">Datasets</h5>
                <div className="datasets p-3">
                    {loaded || <div className="text-center"><i className="fa fa-spinner fa-spin"/> Loading</div>}
                    {loaded && datasets.length == 0 && <div className="text-center">No Datasets</div>}
                    {loaded && datasets.map(dataset => <DatasetComponent key={dataset.name} dataset={dataset}/>)}
                </div>

{/*
                <div className="text-end">
                    <a href="#" className="card-link">Create Dataset</a>
                </div>
*/}
            </div>
        </div>
    )
}