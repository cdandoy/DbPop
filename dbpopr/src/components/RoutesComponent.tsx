import {Route, Routes} from "react-router-dom";
import Datasets from "./datasets/Datasets";
import VirtualFksComponent from "./vfk/VirtualFksComponent";
import EditVirtualFkComponent from "./vfk/EditVirtualFkComponent";
import AddVirtualFkComponent from "./vfk/AddVirtualFkComponent";
import Dashboard from "./dashboard/Dashboard";
import React, {useContext} from "react";
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
import SourceTools from "./tools/SourceTools";
import TargetTools from "./tools/TargetTools";
import TargetDataTools from "./tools/TargetDataTools";
import TargetCodeTools from "./tools/TargetCodeTools";
import SourceCodeTools from "./tools/SourceCodeTools";
import SourceDataTools from "./tools/SourceDataTools";
import SettingsComponent from "./settings/SettingsComponent";
import EditDatabaseSettingsComponent from "./settings/EditDatabaseSettingsComponent";
import {WebSocketStateContext} from "./ws/useWebSocketState";

export default function RoutesComponent() {
    const siteStatus = useContext(WebSocketStateContext);
    return <>
        <Routes>
            <Route path="/datasets" element=<Datasets/>/>

            <Route path="/codechanges" element=<CodeChanges/>/>

            <Route path="/tools" element=<ToolsComponent/>/>
            {siteStatus.hasSource && <>
                <Route path="/tools/source/" element=<SourceTools/>/>
                <Route path="/tools/source/data" element=<SourceDataTools/>/>
                <Route path="/tools/source/code" element=<SourceCodeTools/>/>
                <Route path="/tools/source/structured" element=<StructuredDownloadComponent/>/>
                <Route path="/tools/source/bulk" element=<DownloadBulkComponent/>/>
                <Route path="/tools/source/full" element=<DownloadSourceFullComponent/>/>
                <Route path="/tools/source/compare" element=<CodeSourceCompare/>/>
                <Route path="/tools/source/download" element=<CodeSourceDownload/>/>
            </>}
            {siteStatus.hasTarget && <>
                <Route path="/tools/target/" element=<TargetTools/>/>
                <Route path="/tools/target/data" element=<TargetDataTools/>/>
                <Route path="/tools/target/code" element=<TargetCodeTools/>/>
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
            <Route path="/settings/:type" element=<EditDatabaseSettingsComponent/>/>
            <Route path="/settings" element=<SettingsComponent/>/>

            <Route path="/" element=<Dashboard/>/>
        </Routes>
    </>
}