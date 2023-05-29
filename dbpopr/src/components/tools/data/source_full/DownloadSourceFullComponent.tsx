import React, {useState} from "react";
import useDatasets from "../../../utils/useDatasets";
import LoadingOverlay from "../../../utils/LoadingOverlay";
import axios, {AxiosResponse} from "axios";
import PageHeader from "../../../pageheader/PageHeader";
import source_full from "./source_full.png"
import {DownloadResponse} from "../../../../models/DownloadResponse";
import {Alert} from "react-bootstrap";
import DownloadResultsComponent from "../DownloadResultsComponent";

interface FullDownloadRequest {
    dataset: string;
}

function fullDownload(dataset: string) {
    return axios.post<FullDownloadRequest, AxiosResponse<DownloadResponse>>(`/download/source`, {
        dataset: dataset
    });
}

export default function DownloadSourceFullComponent() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | undefined>();
    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse | undefined>();
    const [datasets, loadingDatasets] = useDatasets();
    const [dataset, setDataset] = useState("base");

    function handleDownload() {
        setLoading(true);
        setError(undefined);
        fullDownload(dataset)
            .then(response => {
                setLoading(false);
                setDownloadResponse(response.data);
            })
            .catch((reason) => {
                setError(reason.response.statusText);
            })
            .finally(() =>
                setLoading(false)
            );
    }

    function DownloadFullInput({error}: {
        error: string | undefined
    }) {
        return <>
            <div className="row g-3 align-items-center">
                <div className="col-auto">
                    <label className={"col-form-label"} htmlFor={"dataset"}>Dataset:</label>
                </div>
                <div className="col-auto">
                    <select id={"dataset"} className="form-select" defaultValue={dataset} onChange={e => setDataset(e.target.value)}>
                        {datasets.map(ds => (
                            <option key={ds} value={ds}>{ds}</option>
                        ))}
                    </select>
                </div>
                <div className="col-auto">
                    <button onClick={handleDownload} className={"btn btn-primary"}>Download</button>
                </div>
            </div>
            {error &&
                <Alert variant={"danger"} className={"mt-3"}>
                    {error}
                </Alert>
            }
        </>
    }

    return <div className={"container"}>
        <LoadingOverlay active={loading || loadingDatasets}/>
        <PageHeader title={"Full Download"}
                    subtitle={"Download a complete database."}
                    tool={<img src={source_full} style={{width: "20em"}} alt={"full download"}/>}/>
        {downloadResponse === undefined && <DownloadFullInput error={error}/>}
        {downloadResponse !== undefined && <DownloadResultsComponent downloadResponse={downloadResponse}/>}
    </div>
}