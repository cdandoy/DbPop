import React, {useEffect, useState} from "react";
import {Configuration} from "../../models/Configuration";
import {Dataset} from "./Dataset";
import {datasetContent, DatasetContentResponse} from "../../api/datasetContent";
import {siteApi} from "../../api/siteApi";
import LoadingOverlay from "../utils/LoadingOverlay";
import './Dashboard.scss'

export default function Dashboard() {
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [configuration, setConfiguration] = useState<Configuration>({hasSource: false, hasTarget: false});
    const [contentResponse, setContentResponse] = useState<DatasetContentResponse | null>(null);
    const [loadingDataset, setLoadingDataset] = useState<string | null>(null);
    const [loadedDataset, setLoadedDataset] = useState<string | null>(null);
    const [loadingResult, setLoadingResult] = useState<string | null>(null);
    const [loadingError, setLoadingError] = useState<string | null>(null);

    useEffect(() => {
        setLoading(true);
        siteApi()
            .then(result => {
                setConfiguration({
                    hasSource: result.data.hasSource,
                    hasTarget: result.data.hasTarget,
                })
                datasetContent()
                    .then((result) => {
                        setLoading(false);
                        setContentResponse(result.data);
                    })
                    .catch(error => {
                        setLoading(false);
                        setError(error);
                    });
            })
            .catch(error => {
                setLoading(false);
                setError(error);
            });
    }, []);

    if (error) return <div className="text-center"><i className="fa fa-error"/> {error}</div>;
    if (!contentResponse) return <></>;
    if (contentResponse.datasetContents.length === 0) return <div className="text-center">No Datasets</div>;

    return (
        <div id={"dashboard"}>
            <LoadingOverlay active={loading}/>
            <div className="text-center m-5">
                <div style={{display: "flex", justifyContent: "center"}}>
                    <h1>Welcome to DbPop </h1>
                </div>
                <p className="lead">The easiest way to populate your development database.</p>
            </div>
            <div className="datasets p-3">
                {contentResponse.datasetContents
                    .filter(it => "static" !== it.name)
                    .map(datasetContent => (
                        <div key={datasetContent.name}>
                            <Dataset
                                datasetContent={datasetContent}
                                hasUpload={configuration.hasTarget}
                                loadingDataset={loadingDataset}
                                loadedDataset={loadedDataset}
                                loadingResult={loadingResult}
                                loadingError={loadingError}
                                setLoadingDataset={setLoadingDataset}
                                setLoadedDataset={setLoadedDataset}
                                setLoadingResult={setLoadingResult}
                                setLoadingError={setLoadingError}
                            />
                        </div>
                    ))}
            </div>
        </div>
    )
}