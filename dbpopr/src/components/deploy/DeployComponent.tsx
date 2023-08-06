import React, {useContext, useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import "./DeployComponent.scss"
import LoadingOverlay from "../utils/LoadingOverlay";
import SqlDownloaded from "./SqlDownloaded";
import {NoChangesContent} from "./NoChangesContent";
import {NoSnapshotContent} from "./NoSnapshotContent";
import {WebSocketStateContext} from "../ws/useWebSocketState";
import {createSnapshot, getDeploy, GetDeployResponse} from "./deployApi";
import FlywayCreated from "./FlywayCreated";
import {DeployInput} from "./DeployInput";

export default function DeployComponent() {
    const [loading, setLoading] = useState(false);
    const [hasSnapshot, setHasSnapshot] = useState(false);
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
        setHasSnapshot(getDeployResponse.hasSnapshot);
        setHasChanges(getDeployResponse.hasChanges);
        setSnapshotFilename(getDeployResponse.snapshotFilename);
        if (getDeployResponse.deltaType) {
            setDefaultType(getDeployResponse.deltaType)
        }
    }

    const handleCreateSnapshot = () => {
        setLoading(true);
        createSnapshot()
            .then(() => getDeploy()
                .then(result => setDeployResult(result.data))
                .finally(() => setLoading(false))
            );
    }

    function handleSqlSnapshot() {
        setLoading(true);
        createSnapshot("SQL")
            .then(() => setState("snapshot-created"))
            .finally(() => setLoading(false));
    }

    function Content() {
        if (!hasSnapshot) return <NoSnapshotContent handleCreateSnapshot={handleCreateSnapshot}/>
        if (!hasChanges) return <NoChangesContent/>
        if (state === "input") return <DeployInput setLoading={setLoading}
                                                   codeChanges={siteStatus.codeChanges}
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
            <PageHeader title={"Deployment"}/>
            {loading || <>
                <div className={"container"}>
                    <Content/>
                </div>
            </>}
        </div>
    </div>;
}