import React from "react";
import DownloadDatasetsComponent from "./DownloadDatasetsComponent";
import DownloadModelsComponent from "./DownloadModelsComponent";

export default function DownloadDashboardComponent(): JSX.Element {
    return (
        <>
            <div className="col-6">
                <DownloadDatasetsComponent/>
            </div>
            <div className="col-6">
                <DownloadModelsComponent/>
            </div>
        </>
    );
}

