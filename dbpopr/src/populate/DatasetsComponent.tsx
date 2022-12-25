import React, {useEffect, useState} from "react";
import './Datasets.scss'
import axios from "axios";
import {PopulateResult} from "../models/PopulateResult";

function DatasetComponent({
                              dataset,
                              loadingDataset,
                              loadedDataset,
                              loadingResult,
                              loadingError,
                              setLoadingDataset,
                              setLoadedDataset,
                              setLoadingResult,
                              setLoadingError,
                          }: {
    dataset: string,

    loadingDataset: string | null,
    loadedDataset: string | null,
    loadingResult: string | null,
    loadingError: string | null,
    setLoadingDataset: (dataset: string | null) => void,
    setLoadedDataset: (dataset: string | null) => void,
    setLoadingResult: (result: string | null) => void,
    setLoadingError: (error: string | null) => void
}): JSX.Element {

    function buttonContent() {
        if (loadingDataset === null) {
            return <i className="fa fa-fw fa-play" onClick={loadDataset}></i>;
        } else if (loadingDataset === dataset) {
            return <i className="fa fa-fw fa-spinner fa-spin"></i>;
        } else {
            return <i className="fa fa-fw" style={{visibility: 'hidden'}}></i>;
        }
    }

    function loadDataset(): void {
        setLoadingDataset(dataset);
        setLoadingResult(null);
        setLoadingError(null);
        axios.get<PopulateResult>("/populate", {params: {dataset}})
            .then(result => {
                setLoadingDataset(null);
                setLoadingResult(`Loaded ${result.data.rows} in ${result.data.millis}ms`);
                setLoadedDataset(dataset);
            })
            .catch(error => {
                setLoadingDataset(null);
                let errorMessages = error.response?.data?._embedded?.errors
                    ?.map((it: any) => {
                        return it?.message;
                    })
                    ?.join('<br/>');
                setLoadingError(errorMessages);
                setLoadedDataset(dataset);
            })
    }

    return (
        <div className="d-flex p-2">
            <div data-dataset={dataset}>
                <div className='dataset-button'>
                    <button className="btn btn-xs button-load" title="Load">
                        {buttonContent()}
                    </button>
                    <span>{dataset}</span>
                </div>
                {loadedDataset === dataset && loadingError && <pre className='dataset-error'>{loadingError}</pre>}
                {loadedDataset === dataset && loadingResult && <div className='dataset-result'>{loadingResult}</div>}
            </div>
        </div>
    );
}

export default function DatasetsComponent(): JSX.Element {
    const [datasets, setDatasets] = useState<string[]>([]);
    const [loadingDataset, setLoadingDataset] = useState<string | null>(null);
    const [loadedDataset, setLoadedDataset] = useState<string | null>(null);
    const [loadingResult, setLoadingResult] = useState<string | null>(null);
    const [loadingError, setLoadingError] = useState<string | null>(null);

    useEffect(() => {
        axios.get<string[]>("/datasets")
            .then((result) => {
                setDatasets(result.data);
            });
    }, []);

    function isDisplayable(dataset: string): boolean {
        return dataset !== 'static'
    }

    return (
        <>
            <div className="tab-pane active"
                 id="datasets-tab-pane"
                 role="tabpanel"
                 aria-labelledby="datasets-tab"
                 tabIndex={0}>
                <div className="datasets p-2">
                    {datasets.map(dataset => isDisplayable(dataset) &&
                        <DatasetComponent key={dataset}
                                          dataset={dataset}
                                          loadingDataset={loadingDataset}
                                          loadedDataset={loadedDataset}
                                          loadingResult={loadingResult}
                                          loadingError={loadingError}
                                          setLoadingDataset={setLoadingDataset}
                                          setLoadedDataset={setLoadedDataset}
                                          setLoadingResult={setLoadingResult}
                                          setLoadingError={setLoadingError}
                        />)}
                </div>
            </div>
        </>
    );
}