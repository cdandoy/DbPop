import React, {useState} from "react";
import {DownloadResponse} from "../../../models/DownloadResponse";
import SelectTables from "./SelectTables";
import DownloadResultComponent from "./DownloadResultComponent";
import {useLocalStorage} from "usehooks-ts";
import SelectTableDependenciesComponent from "../SelectTableDependenciesComponent";
import {TableName} from "../../../models/TableName";

export default function DownloadBulkComponent() {
    const [schema, setSchema] = useLocalStorage("last-schema", "")
    const [nameFilter, setNameFilter] = useState("");
    const [nameRegExp, setNameRegExp] = useState<RegExp>(/.*/);
    const [emptyFilter, setEmptyFilter] = useState(false);
    const [downloadedFilter, setDownloadedFilter] = useState(false);
    const [dependenciesFilter, setDependenciesFilter] = useState(false);
    const [showTableNameDependency, setShowTableNameDependency] = useState<TableName | undefined>();

    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse | null>(null);
    const [dataset, setDataset] = useState("static")

    if (downloadResponse == null) {
        return (
            <>
                <SelectTableDependenciesComponent tableName={showTableNameDependency} close={() => setShowTableNameDependency(undefined)}/>
                <SelectTables
                    schema={schema} setSchema={setSchema}
                    nameFilter={nameFilter} setNameFilter={setNameFilter}
                    nameRegExp={nameRegExp} setNameRegExp={setNameRegExp}
                    emptyFilter={emptyFilter} setEmptyFilter={setEmptyFilter}
                    downloadedFilter={downloadedFilter}
                    setDownloadedFilter={setDownloadedFilter}
                    dependenciesFilter={dependenciesFilter} setDependenciesFilter={setDependenciesFilter}
                    dataset={dataset} setDataset={setDataset}
                    setDownloadResponse={setDownloadResponse}
                    setShowTableNameDependency={setShowTableNameDependency}
                />
            </>
        )
    } else {
        return <DownloadResultComponent downloadResponse={downloadResponse} setDownloadResponse={setDownloadResponse}/>
    }
}