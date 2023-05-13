import React, {useState} from "react";
import LoadingOverlay from "../../../utils/LoadingOverlay";
import PageHeader from "../../../pageheader/PageHeader";
import {DownloadResponse} from "../../../../models/DownloadResponse";
import useDatasets from "../../../utils/useDatasets";
import target_full from "./target_full.png";
import axios, {AxiosResponse} from "axios";
import {Dependency} from "../../../../models/Dependency";
import {Alert} from "react-bootstrap";
import DownloadResultsComponent from "../DownloadResultsComponent";

function dowloadTarget(dataset: string): Promise<AxiosResponse<DownloadResponse>> {

    return axios.post<Dependency, AxiosResponse<DownloadResponse>>(`/download/target`, {
        dataset,
        dryRun: false,
        maxRows: 2147483648
    })
}

export default function DownloadTargetFullComponent() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | undefined>();
    const [datasets, loadingDatasets] = useDatasets();
    const [dataset, setDataset] = useState('base');
    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse>();

    function handleDownload() {
        setLoading(true);
        dowloadTarget(dataset)
            .then(result => {
                setDownloadResponse(result.data);
            })
            .catch(reason => {
                setError(reason.response.statusText);
            })
            .finally(() => {
                setLoading(false);
            })
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

    return <div id={"download-target"}>
        <LoadingOverlay active={loading || loadingDatasets}/>
        <PageHeader title={"Full Target Download"}
                    subtitle={"Download the target database."}
                    tool={<img src={target_full} style={{width: "20em"}} alt={"full download"}/>}/>
        {!downloadResponse && <DownloadFullInput error={error}/>}
        {downloadResponse && <DownloadResultsComponent downloadResponse={downloadResponse}/>}
    </div>
}