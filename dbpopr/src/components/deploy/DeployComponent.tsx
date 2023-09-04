import React, {useContext, useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import "./DeployComponent.scss"
import LoadingOverlay from "../utils/LoadingOverlay";
import SqlDownloaded from "./SqlDownloaded";
import {NoChangesContent} from "./NoChangesContent";
import {WebSocketStateContext} from "../ws/useWebSocketState";
import {createSnapshot, getDeploy, GetDeployResponse} from "./deployApi";
import FlywayCreated from "./FlywayCreated";
import {DeployInput} from "./DeployInput";
import {NavLink} from "react-router-dom";

export default function DeployComponent() {
    const [loading, setLoading] = useState(false);
    const [hasChanges, setHasChanges] = useState(false);
    const [state, setState] = useState("input")
    const [snapshotFilename, setSnapshotFilename] = useState("");
    const [defaultType, setDefaultType] = useState("SQL");
    const [flywayGeneratedFilename, setFlywayGeneratedFilename] = useState("");
    const siteStatus = useContext(WebSocketStateContext);

    useEffect(() => {
        setLoading(true);
        getDeploy()
            .then(result => setDeployResult(result.data))
            .finally(() => setLoading(false));
    }, [siteStatus.codeChanged]);

    function setDeployResult(getDeployResponse: GetDeployResponse) {
        setHasChanges(getDeployResponse.hasChanges);
        setSnapshotFilename(getDeployResponse.snapshotFilename);
        if (getDeployResponse.deltaType) {
            setDefaultType(getDeployResponse.deltaType)
        }
    }

    function handleSqlSnapshot() {
        setLoading(true);
        createSnapshot("SQL")
            .then(() => setState("snapshot-created"))
            .finally(() => setLoading(false));
    }

    function Tool() {
        return <div>
            <NavLink className={"btn btn-sm btn-danger"} to={"/deployment/reset"}>
                <span title={"Reset the deployments"}><i className={"fa fa-undo"}/> Reset...</span>
            </NavLink>
        </div>
    }

    function Content() {
        if (!hasChanges) return <NoChangesContent/>
        if (state === "input") return <DeployInput setLoading={setLoading}
                                                   setState={setState}
                                                   setFlywayGeneratedFilename={setFlywayGeneratedFilename}
                                                   defaultType={defaultType}/>
        if (state === "sql-downloaded") return <SqlDownloaded snapshotFilename={snapshotFilename}
                                                              onCancel={() => setState("input")}
                                                              onOk={handleSqlSnapshot}/>
        if (state === "snapshot-created") return <div>Snapshot Created</div>
        if (state === "flyway-created") return <FlywayCreated flywayGeneratedFilename={flywayGeneratedFilename}
                                                              snapshotFilename={snapshotFilename}/>
        return <>Unknown State: hasSnapshot: {state}</>
    }

    return <div id={"deployment-component"}>
        <LoadingOverlay active={loading}/>

        <div className={"container"}>
            <PageHeader title={"Deployment"} tool={<Tool/>}/>
            {loading || <>
                <div className={"container"}>
                    <Content/>
                </div>
            </>}
        </div>
    </div>;
}