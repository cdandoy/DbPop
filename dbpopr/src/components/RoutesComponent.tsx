import {Route, Routes} from "react-router-dom";
import Datasets from "./datasets/Datasets";
import VirtualFksComponent from "./vfk/VirtualFksComponent";
import EditVirtualFkComponent from "./vfk/EditVirtualFkComponent";
import AddVirtualFkComponent from "./vfk/AddVirtualFkComponent";
import Dashboard from "./dashboard/Dashboard";
import React, {useContext} from "react";
import ToolsComponent from "./tools/ToolsComponent";
import DownloadBulkComponent from "./tools/data/source_bulk/DownloadBulkComponent";
import DownloadSourceFullComponent from "./tools/data/source_full/DownloadSourceFullComponent";
import StructuredDownloadComponent from "./tools/data/source_structured/StructuredDownloadComponent";
import DownloadTargetFullComponent from "./tools/data/target_full/DownloadTargetFullComponent";
import SettingsComponent from "./settings/SettingsComponent";
import EditDatabaseSettingsComponent from "./settings/EditDatabaseSettingsComponent";
import {WebSocketStateContext} from "./ws/useWebSocketState";

export default function RoutesComponent() {
    const siteStatus = useContext(WebSocketStateContext);
    return <>
        <Routes>
            <Route path="/datasets" element=<Datasets/>/>

            <Route path="/tools" element=<ToolsComponent/>/>
            {siteStatus.hasSource && <>
                <Route path="/tools/source/structured" element=<StructuredDownloadComponent/>/>
                <Route path="/tools/source/bulk" element=<DownloadBulkComponent/>/>
                <Route path="/tools/source/full" element=<DownloadSourceFullComponent/>/>
            </>}
            {siteStatus.hasTarget && <>
                <Route path="/tools/target/full" element=<DownloadTargetFullComponent/>/>
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