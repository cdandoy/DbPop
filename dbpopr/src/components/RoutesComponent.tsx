import {Route, Routes} from "react-router-dom";
import Datasets from "./datasets/Datasets";
import VirtualFksComponent from "./vfk/VirtualFksComponent";
import EditVirtualFkComponent from "./vfk/EditVirtualFkComponent";
import AddVirtualFkComponent from "./vfk/AddVirtualFkComponent";
import Dashboard from "./dashboard/Dashboard";
import React, {useContext} from "react";
import {SiteContext} from "./app/App";
import ToolsComponent from "./tools/ToolsComponent";
import CodeSourceDownload from "./tools/code/sourcedownload/CodeSourceDownload";
import CodeSourceCompare from "./tools/code/sourcecompare/CodeSourceCompare";
import CodeTargetCompare from "./tools/code/targetcompare/CodeTargetCompare";
import CodeTargetDownload from "./tools/code/targetdownload/CodeTargetDownload";
import CodeTargetUload from "./tools/code/targetupload/CodeTargetUload";
import DownloadBulkComponent from "./tools/data/source_bulk/DownloadBulkComponent";
import DownloadSourceFullComponent from "./tools/data/source_full/DownloadSourceFullComponent";
import StructuredDownloadComponent from "./tools/data/source_structured/StructuredDownloadComponent";
import DownloadTargetFullComponent from "./tools/data/target_full/DownloadTargetFullComponent";
import CodeChanges from "./codechanges/CodeChanges";
import {CodeCompare} from "./codecompare/CodeCompare";
import DeployComponent from "./deploy/DeployComponent";

export default function RoutesComponent() {
    const siteResponse = useContext(SiteContext);
    return <>
        <Routes>
            <Route path="/datasets" element=<Datasets/>/>

            <Route path="/codechanges" element=<CodeChanges/>/>

            <Route path="/tools" element=<ToolsComponent/>/>
            {siteResponse.hasSource && <>
                <Route path="/tools/source/structured" element=<StructuredDownloadComponent/>/>
                <Route path="/tools/source/bulk" element=<DownloadBulkComponent/>/>
                <Route path="/tools/source/full" element=<DownloadSourceFullComponent/>/>
                <Route path="/tools/source/compare" element=<CodeSourceCompare/>/>
                <Route path="/tools/source/download" element=<CodeSourceDownload/>/>
            </>}
            {siteResponse.hasTarget && <>
                <Route path="/tools/target/full" element=<DownloadTargetFullComponent/>/>
                <Route path="/tools/target/compare" element=<CodeTargetCompare/>/>
                <Route path="/tools/target/upload" element=<CodeTargetUload/>/>
                <Route path="/tools/target/download" element=<CodeTargetDownload/>/>
                <Route path="/codechanges/diff" element=<CodeCompare/>/>
                <Route path="/deployment" element=<DeployComponent/>/>
            </>}

            <Route path="/vfk" element=<VirtualFksComponent/>/>
            <Route path="/vfk/add" element=<AddVirtualFkComponent/>/>
            <Route path="/vfk/:pkTable/:fkName" element=<EditVirtualFkComponent/>/>
            <Route path="/" element=<Dashboard/>/>
        </Routes>
    </>
}