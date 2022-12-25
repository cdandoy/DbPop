import {NavLink} from "react-router-dom";
import React, {useEffect, useState} from "react";
import axios from "axios";
import {DatasetResponse} from "../models/DatasetResponse";
import {toHumanReadableSize} from "../utils/DbPopUtils";

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
        <>
            <tr>
                <td>
                    <NavLink to={`download/${dataset.name}`}>
                        {dataset.name}
                    </NavLink>
                </td>
                <td className="text-end">{files.length}</td>
                <td className="text-end">{rows}</td>
                <td className="text-end">{readableSize.text}</td>
            </tr>
        </>
    );
}

export default function DownloadDatasetsComponent() {
    const [datasets, setDatasets] = useState<DatasetResponse[]>([]);
    useEffect(() => {
        axios.get<DatasetResponse[]>("/datasets/content")
            .then((result) => setDatasets(result.data));
    }, []);
    return (
        <div className="card datasets">
            <div className="card-body">
                <h5 className="card-title">Datasets</h5>
                <div className="datasets p-3">
                    {datasets.length == 0 && <div className="text-center">No Datasets</div>}
                    {datasets.length > 0 &&
                        <table className="table table-hover">
                            <thead>
                            <tr>
                                <th>Dataset</th>
                                <th className="text-end">Files</th>
                                <th className="text-end">Rows</th>
                                <th className="text-end">Size</th>
                            </tr>
                            </thead>
                            <tbody>
                            {datasets.map(dataset => <DatasetComponent key={dataset.name}
                                                                       dataset={dataset}
                            />)}
                            </tbody>
                        </table>
                    }
                </div>

                <div className="text-end">
                    <a href="#" className="card-link">Create Dataset</a>
                </div>
            </div>
        </div>
    )
}