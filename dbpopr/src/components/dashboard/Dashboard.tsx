import React, {useEffect, useState} from "react";
import {Configuration} from "../../models/Configuration";
import {Dataset} from "./Dataset";
import {datasetContent, DatasetContentResponse} from "../../api/datasetContent";
import {siteApi} from "../../api/siteApi";
import LoadingOverlay from "../utils/LoadingOverlay";
import './Dashboard.scss'
import {populate} from "../../api/Populate";

export default function Dashboard() {
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [configuration, setConfiguration] = useState<Configuration>({hasSource: false, hasTarget: false});
    const [contentResponse, setContentResponse] = useState<DatasetContentResponse | null>(null);
    const [loadingDataset, setLoadingDataset] = useState<string | null>(null);
    const [datasetContentCN, setDatasetContentCN] = useState(0);

    useEffect(() => {
        setLoading(true);
        siteApi()
            .then(result => {
                setConfiguration({
                    hasSource: result.data.hasSource,
                    hasTarget: result.data.hasTarget,
                });
                setDatasetContentCN(datasetContentCN + 1);
            })
            .catch(error => {
                setLoading(false);
                setError(error);
            });
    }, []);

    useEffect(() => {
        datasetContent()
            .then((result) => setContentResponse(result.data))
            .catch(error => setError(error))
            .finally(() => setLoading(false));
    }, [datasetContentCN])

    if (error) return <div className="text-center"><i className="fa fa-error"/> {error}</div>;
    if (!contentResponse) return <></>;
    if (contentResponse.datasetContents.length === 0) return <div className="text-center">No Datasets</div>;

    function loadDataset(datasetName: string): void {
        setLoadingDataset(datasetName);
        populate(datasetName)
            .then(() => setLoadingDataset(null))
            .catch(() => setLoadingDataset(null))
            .finally(() => setDatasetContentCN(datasetContentCN + 1))
    }


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
                    .filter(datasetContent => datasetContent.failureCauses || "static" !== datasetContent.name)
                    .map(datasetContent => (
                        <div key={datasetContent.name}>
                            <Dataset
                                datasetContent={datasetContent}
                                hasUpload={configuration.hasTarget}
                                loadingDataset={loadingDataset}
                                loadDataset={loadDataset}
                            />
                        </div>
                    ))}
            </div>
        </div>
    )
}