import React from "react";
import {Route, Routes} from "react-router-dom";
import './DownloadComponent.scss'
import DownloadDatasetComponent from "./DownloadDatasetComponent";
import DownloadDashboardComponent from "./DownloadDashboardComponent";

function DownloadComponent(): JSX.Element {
    return (
        <>
            <div id="download-component" className="row">
                <Routes>
                    <Route path="/download/:dataset" element=<DownloadDatasetComponent/>/>
                    <Route path="/" element=<DownloadDashboardComponent/>/>
                </Routes>
            </div>
        </>
    )
}

export default DownloadComponent;
