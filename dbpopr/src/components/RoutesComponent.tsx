import {Route, Routes} from "react-router-dom";
import Datasets from "./datasets/Datasets";
import DownloadComponent from "./download/DownloadComponent";
import DownloadBulkComponent from "./download/source_bulk/DownloadBulkComponent";
import StructuredDownloadComponent from "./download/structured/StructuredDownloadComponent";
import VirtualFksComponent from "./vfk/VirtualFksComponent";
import EditVirtualFkComponent from "./vfk/EditVirtualFkComponent";
import AddVirtualFkComponent from "./vfk/AddVirtualFkComponent";
import Dashboard from "./dashboard/Dashboard";
import React, {useContext} from "react";
import DownloadTargetFullComponent from "./download/target_full/DownloadTargetFullComponent";
import CodeComponent from "./code/CodeComponent";
import CodeSourceDownload from "./code/CodeSourceDownload";
import CodeSourceCompare from "./code/CodeSourceCompare";
import CodeTargetUload from "./code/CodeTargetUload";
import CodeTargetDownload from "./code/CodeTargetDownload";
import CodeTargetCompare from "./code/CodeTargetCompare";
import DownloadSourceFullComponent from "./download/source_full/DownloadSourceFullComponent";
import {SiteContext} from "./app/App";

export default function RoutesComponent() {
    const siteResponse = useContext(SiteContext);
    return <>
        <Routes>
            <Route path="/datasets" element=<Datasets/>/>

            <Route path="/download" element=<DownloadComponent/>/>
            {siteResponse.hasSource && <>
                <Route path="/download/structured" element=<StructuredDownloadComponent/>/>
                <Route path="/download/bulk" element=<DownloadBulkComponent/>/>
                <Route path="/download/full" element=<DownloadSourceFullComponent/>/>
            </>}
            {siteResponse.hasTarget && <>
                <Route path="/download/target" element=<DownloadTargetFullComponent/>/>
            </>}

            <Route path="/code" element=<CodeComponent/>/>
            <Route path="/code/source/compare" element=<CodeSourceCompare/>/>
            <Route path="/code/source/download" element=<CodeSourceDownload/>/>
            <Route path="/code/target/compare" element=<CodeTargetCompare/>/>
            <Route path="/code/target/upload" element=<CodeTargetUload/>/>
            <Route path="/code/target/download" element=<CodeTargetDownload/>/>

            <Route path="/vfk" element=<VirtualFksComponent/>/>
            <Route path="/vfk/add" element=<AddVirtualFkComponent/>/>
            <Route path="/vfk/:pkTable/:fkName" element=<EditVirtualFkComponent/>/>
            <Route path="/" element=<Dashboard/>/>
        </Routes>
    </>
}