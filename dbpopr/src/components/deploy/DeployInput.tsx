import React, {useContext, useEffect, useState} from "react";
import SqlForm from "./SqlForm";
import FlywayForm from "./FlywayForm";
import {NavLink} from "react-router-dom";
import axios from "axios";
import {ChangedObject} from "../codechanges/CodeChanges";
import {WebSocketStateContext} from "../ws/useWebSocketState";

export function DeployInput({setLoading, setState, setFlywayGeneratedFilename, defaultType}: {
    setLoading: (b: boolean) => void
    setState: (s: string) => void
    setFlywayGeneratedFilename: (s: string) => void
    defaultType: string
}) {
    const siteStatus = useContext(WebSocketStateContext);
    const [type, setType] = useState(defaultType);
    const [hasDbChanges, setHasDbChanges] = useState(false);

    useEffect(() => {
        axios.get<ChangedObject[]>(`/codechanges/target`)
            .then(response => {
                setHasDbChanges(
                    response.data.filter(change => change.changeType === "DATABASE_ONLY" || change.changeType === "UPDATED").length > 0
                )
            });
    }, [siteStatus.codeDiffChanges]);

    return <div className={"row"}>
        <div className={"col-6"}>
            {hasDbChanges && <div className="alert alert-warning" role="alert">
                <i className={"fa fa-exclamation-triangle"}/> You have <NavLink to={"/codechanges"}>changes</NavLink> in the database that have not been saved to the code directory.
            </div>}
            <div className={"mb-3"}>
                <label htmlFor="deployType">Deployment Type</label>
                <select className="form-select" defaultValue={type} onChange={e => setType(e.target.value)}>
                    <option value={"SQL"}>SQL Scripts</option>
                    <option value={"FLYWAY"}>Flyway</option>
                </select>
                {type === "SQL"
                    ? <small className="form-text text-muted">Generates a pair of SQL Scripts to deploy and undeploy your changes.</small>
                    : <small className="form-text text-muted">Generates a <a href={"https://flywaydb.org/"} target={"_blank"} rel="noreferrer">Flyway</a> script.</small>
                }
            </div>

            {type === "SQL" && <SqlForm setLoading={setLoading} setState={setState}/>}
            {type === "FLYWAY" && <FlywayForm setLoading={setLoading} setState={setState} setFlywayGeneratedFilename={setFlywayGeneratedFilename}/>}
        </div>
    </div>;
}