import React, {useState} from "react";
import {DownloadResponse} from "../../../models/DownloadResponse";
import SelectTables from "./SelectTables";
import DownloadResultComponent from "./DownloadResultComponent";

export default function DownloadBulkComponent() {
    const [nameFilter, setNameFilter] = useState("");
    const [nameRegExp, setNameRegExp] = useState<RegExp>(/.*/);
    const [emptyFilter, setEmptyFilter] = useState(false);
    const [downloadedFilter, setDownloadedFilter] = useState(false);
    const [dependenciesFilter, setDependenciesFilter] = useState(false);

    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse | null>(null);
    const [dataset, setDataset] = useState("static")

    if (downloadResponse == null) {
        return <SelectTables
            nameFilter={nameFilter} setNameFilter={setNameFilter}
            nameRegExp={nameRegExp} setNameRegExp={setNameRegExp}
            emptyFilter={emptyFilter} setEmptyFilter={setEmptyFilter}
            downloadedFilter={downloadedFilter}
            setDownloadedFilter={setDownloadedFilter}
            dependenciesFilter={dependenciesFilter} setDependenciesFilter={setDependenciesFilter}
            dataset={dataset} setDataset={setDataset}
            setDownloadResponse={setDownloadResponse}
        />
    } else {
        return <DownloadResultComponent downloadResponse={downloadResponse} setDownloadResponse={setDownloadResponse}/>
    }
}