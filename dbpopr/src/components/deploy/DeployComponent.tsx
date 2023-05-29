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
    const messageState = useContext(WebSocketStateContext);

    useEffect(() => {
        setLoading(true);
        getDeploy()
            .then(result => setDeployResult(result.data))
            .finally(() => setLoading(false));
    }, [messageState.codeChanged]);

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

    return <div id={"deployment-component"}>
        <LoadingOverlay active={loading}/>

        <div className={"deploy-right"}>
            <PageHeader title={"Deployment"}/>
            {loading || <>
                <div className={"container"}>
                    {hasSnapshot && hasChanges && <>
                        {state === "input" && <DeployInput setLoading={setLoading} setState={setState} setFlywayGeneratedFilename={setFlywayGeneratedFilename} defaultType={defaultType}/>}
                        {state === "sql-downloaded" && <SqlDownloaded snapshotFilename={snapshotFilename} onCancel={() => setState("input")} onOk={handleSqlSnapshot}/>}
                        {state === "snapshot-created" && <div>Snapshot Created</div>}
                        {state === "flyway-created" && <FlywayCreated flywayGeneratedFilename={flywayGeneratedFilename} snapshotFilename={snapshotFilename}/>}
                    </>
                    }
                    {(!hasSnapshot) && <NoSnapshotContent handleCreateSnapshot={handleCreateSnapshot}/>}
                    {(!hasChanges) && <NoChangesContent/>}
                </div>
            </>}
        </div>
    </div>;
}