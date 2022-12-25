import React, {useEffect, useState} from "react";
import axios from "axios";

interface DatasetFileRow {
    //String datasetName, String tableName, Long fileSize, Integer rows
    datasetName: string;
    tableName: string;
    fileSize: number;
    rows: number;
}

export function FilesComponent() {
    const [datasets, setDatasets] = useState<DatasetFileRow[]>([]);

    useEffect(() => {
        axios.get<DatasetFileRow[]>("/datasets/files")
            .then((result) => {
                setDatasets(result.data);
            });
    }, []);

    return (
        <div>
            <table className="table table-hover mt-3">
                <thead>
                <tr>
                    <th>Dataset</th>
                    <th>Table</th>
                    <th>Rows</th>
                    <th>Size</th>
                </tr>
                </thead>
                <tbody>
                {datasets.map(it => (
                    <tr key={it.datasetName}>
                        <td>{it.datasetName}</td>
                        <td>{it.tableName}</td>
                        <td>{it.rows}</td>
                        <td>{it.fileSize}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}