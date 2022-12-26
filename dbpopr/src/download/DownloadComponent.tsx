import React from "react";
import {Route, Routes} from "react-router-dom";
import './DownloadComponent.scss'
import DownloadDatasetDetailsComponent from "./DownloadDatasetDetailsComponent";
import DownloadDashboardComponent from "./DownloadDashboardComponent";
import DownloadAdd from "./DownloadAdd";

function DownloadComponent(): JSX.Element {
    return (
        <>
            <div id="download-component" className="row">
                <Routes>
                    <Route path="/dataset/:dataset" element=<DownloadDatasetDetailsComponent/>/>
                    <Route path="/add/:datasetName" element=<DownloadAdd/>/>
                    <Route path="/" element=<DownloadDashboardComponent/>/>
                </Routes>
            </div>
        </>
    )
}

export default DownloadComponent;
