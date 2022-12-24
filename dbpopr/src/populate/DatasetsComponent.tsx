import React, {useState} from "react";
import './Datasets.scss'

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

    function loadingComponent() {
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
        setTimeout(() => {
            setLoadingDataset(null);
            setLoadedDataset(dataset);
            setLoadingError('Something is wrong');
        }, 2000);
    }

    return (
        <div className="d-flex p-2">
            <div data-dataset={dataset}>
                <div className='dataset-button'>
                    <button className="btn btn-xs button-load" title="Load">
                        {loadingComponent()}
                    </button>
                    <span>{dataset}</span>
                </div>
                {loadedDataset === dataset && loadingError && <div className='dataset-error'>{loadingError}</div>}
                {loadedDataset === dataset && loadingResult && <div className='dataset-result'>{loadingResult}</div>}
            </div>
        </div>
    );
}

export default function DatasetsComponent(): JSX.Element {
    const [datasets, setDatasets] = useState<string[]>(['static', 'base']);
    const [loadingDataset, setLoadingDataset] = useState<string | null>(null);
    const [loadedDataset, setLoadedDataset] = useState<string | null>(null);
    const [loadingResult, setLoadingResult] = useState<string | null>(null);
    const [loadingError, setLoadingError] = useState<string | null>(null);

    return (
        <>
            <div className="tab-pane active"
                 id="datasets-tab-pane"
                 role="tabpanel"
                 aria-labelledby="datasets-tab"
                 tabIndex={0}>
                <div className="datasets p-2">
                    {datasets.map(dataset => <DatasetComponent key={dataset}
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
    )
}