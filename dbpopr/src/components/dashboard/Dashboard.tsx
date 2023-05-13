import React, {useContext, useEffect, useState} from "react";
import {Dataset} from "./Dataset";
import {datasetContent, DatasetContentResponse} from "../../api/datasetContent";
import LoadingOverlay from "../utils/LoadingOverlay";
import './Dashboard.scss'
import {populate} from "../../api/Populate";
import {SiteContext} from "../app/App";
import CodeChanges from "../codechanges/CodeChanges";

export default function Dashboard() {
    const siteResponse = useContext(SiteContext);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [contentResponse, setContentResponse] = useState<DatasetContentResponse | null>(null);
    const [loadingDataset, setLoadingDataset] = useState<string | null>(null);
    const [datasetContentCN, setDatasetContentCN] = useState(0);

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
            {siteResponse.hasTarget && (
                <div className="datasets p-3">
                    {contentResponse.datasetContents
                        .filter(datasetContent => datasetContent.failureCauses || "static" !== datasetContent.name)
                        .map(datasetContent => (
                            <div key={datasetContent.name}>
                                <Dataset
                                    datasetContent={datasetContent}
                                    loadingDataset={loadingDataset}
                                    loadDataset={loadDataset}
                                />
                            </div>
                        ))}
                </div>
            )}
            {siteResponse.hasTarget || (
                <h2>No target database defined</h2>
            )}
            <CodeChanges/>
        </div>
    )
}