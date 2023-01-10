import {DatasetResponse} from "../../models/DatasetResponse";
import {toHumanReadableSize} from "../../utils/DbPopUtils";
import {NavLink} from "react-router-dom";
import React from "react";
import axios from "axios";
import {PopulateResult} from "../../models/PopulateResult";

function Plural({count, text}: {
    count: number;
    text: string;
}) {
    if (count === 1) return <>1 {text}</>
    return <>{count} {text}s</>
}

export function Dataset({
                            dataset,
                            hasDownload,
                            hasUpload,
                            loadingDataset,
                            loadedDataset,
                            loadingResult,
                            loadingError,
                            setLoadingDataset,
                            setLoadedDataset,
                            setLoadingResult,
                            setLoadingError,
                        }: {
    dataset: DatasetResponse;
    hasDownload: boolean;
    hasUpload: boolean;
    loadingDataset: string | null,
    loadedDataset: string | null,
    loadingResult: string | null,
    loadingError: string | null,
    setLoadingDataset: (dataset: string | null) => void,
    setLoadedDataset: (dataset: string | null) => void,
    setLoadingResult: (result: string | null) => void,
    setLoadingError: (error: string | null) => void
}) {
    const files = dataset.files;
    let size = 0;
    let rows = 0;
    for (const file of files) {
        size += file.fileSize;
        rows += file.rows;
    }
    let readableSize = toHumanReadableSize(size);

    function buttonContent() {
        if (dataset.name === "static") return <div style={{display: "inline", paddingLeft: "23px"}}>&nbsp;</div>;
        if (loadingDataset === null) {
            if (!hasUpload) {
                return (
                    <button className="btn btn-xs btn-primary" title="Target database not available" disabled={true}>
                        <i className="fa fa-fw fa-refresh"/>
                    </button>
                )
            }

            return (
                <button className="btn btn-xs btn-primary" title="Reload" onClick={loadDataset}>
                    <i className="fa fa-fw fa-refresh"/>
                </button>
            )
        } else {
            if (loadingDataset === dataset.name) {
                return (
                    <button className="btn btn-xs btn-primary" disabled={true}>
                        <i className="fa fa-fw fa-spinner fa-spin"/>
                    </button>
                );
            } else {
                return (
                    <button className="btn btn-xs btn-primary" disabled={true}>
                        <i className="fa fa-fw fa-refresh"/>
                    </button>
                );
            }
        }
    }

    function statusContent() {
        if (loadedDataset === dataset.name) {
            if (loadingError) {
                return (
                    <div className="mb-2">
                        <pre className="dataset-error">{loadingError}</pre>
                    </div>
                )
            } else if (loadingResult) {
                return (
                    <div className="mb-2">
                        <div className='dataset-result'>{loadingResult}</div>
                    </div>
                )
            } else {
                return (
                    <div className="mb-2">
                        <div className='dataset-result'>&nbsp;</div>
                    </div>
                )
            }
        } else {
            return (
                <div className="mb-2 text-muted">
                    <Plural count={files.length} text="file"/>
                    <>,&nbsp;</>
                    <Plural count={rows} text="row"/>
                    <>,&nbsp;</>
                    {readableSize.text}
                </div>
            )
        }
    }

    function loadDataset(): void {
        setLoadingDataset(dataset.name);
        setLoadingResult(null);
        setLoadingError(null);
        axios.get<PopulateResult>("/populate", {params: {dataset: dataset.name}})
            .then(result => {
                setLoadingDataset(null);
                const rows = result.data.rows + (result.data.rows === 1 ? ' row' : ' rows')
                setLoadingResult(`Loaded ${rows} in ${result.data.millis}ms`);
                setLoadedDataset(dataset.name);
            })
            .catch(error => {
                setLoadingDataset(null);
                let errorMessages = error.response?.data?._embedded?.errors
                    ?.map((it: any) => {
                        return it?.message;
                    })
                    ?.join('<br/>');
                setLoadingError(errorMessages);
                setLoadedDataset(dataset.name);
            })
    }

    const cardStyle = loadedDataset === dataset.name ? {borderColor: "limegreen", borderWidth: "2px"} : {};
    return (
        <div className="card m-3" style={cardStyle}>
            <div className="card-body">
                <div className="row mb-2">
                    <div className="col-9">
                        {buttonContent()}
                        <strong className="ms-1" style={{fontSize: "120%"}}>{dataset.name}</strong>
                    </div>
                    <div className="col-3 text-end">
                        <NavLink to={`dataset/${dataset.name}`} className="btn btn-xs btn-primary" title="Information">
                            <i className="fa fa-fw fa-info"/>
                        </NavLink>
                        {hasDownload &&
                            <NavLink to={`add/${dataset.name}`} className="btn btn-xs btn-primary ms-1" title="Add Data">
                                <i className="fa fa-fw fa-plus"/>
                            </NavLink>
                        }
                    </div>
                </div>
                <div style={{marginLeft: "32px"}}>
                    {statusContent()}
                </div>
            </div>
        </div>
    )
}