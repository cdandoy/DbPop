import {Plural, toHumanReadableSize} from "../../utils/DbPopUtils";
import React from "react";
import axios from "axios";
import {PopulateResult} from "../../models/PopulateResult";
import {DatasetContent} from "../../api/datasetContent";

export function Dataset({
                            datasetContent,
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
    datasetContent: DatasetContent;
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
    let readableSize = toHumanReadableSize(datasetContent.size);

    const datasetName = datasetContent.name;

    function buttonContent() {
        if (datasetName === "static") return <div style={{display: "inline", paddingLeft: "23px"}}>&nbsp;</div>;
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
            if (loadingDataset === datasetName) {
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
        if (loadedDataset === datasetName) {
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
                    {Plural(datasetContent.fileCount, 'table')}
                    <>,&nbsp;</>
                    {Plural(datasetContent.rows, 'row')}
                    <>,&nbsp;</>
                    {readableSize.text}
                </div>
            )
        }
    }

    function loadDataset(): void {
        setLoadingDataset(datasetName);
        setLoadingResult(null);
        setLoadingError(null);
        axios.get<PopulateResult>("/populate", {params: {dataset: datasetName}})
            .then(result => {
                setLoadingDataset(null);
                const rows = result.data.rows + (result.data.rows === 1 ? ' row' : ' rows')
                setLoadingResult(`Loaded ${rows} in ${result.data.millis}ms`);
                setLoadedDataset(datasetName);
            })
            .catch(error => {
                setLoadingDataset(null);
                let errorMessages = error.response?.data?._embedded?.errors
                    ?.map((it: any) => {
                        return it?.message;
                    })
                    ?.join('<br/>');
                setLoadingError(errorMessages);
                setLoadedDataset(datasetName);
            })
    }

    const cardStyle = loadedDataset === datasetName ? {borderColor: "limegreen", borderWidth: "2px"} : {};
    return (
        <div className="card m-3" style={cardStyle}>
            <div className="card-body">
                <div className="row mb-2">
                    <div className="col-9">
                        {buttonContent()}
                        <strong className="ms-1" style={{fontSize: "120%"}}>{datasetName}</strong>
                    </div>
                </div>
                <div style={{marginLeft: "32px"}}>
                    {statusContent()}
                </div>
            </div>
        </div>
    )
}